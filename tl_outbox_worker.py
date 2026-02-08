import os, sys, time, traceback, telethon, telethon.sync, utility as utl, random
from telethon.errors import FloodWaitError
import logging
from logging.handlers import RotatingFileHandler

directory = os.path.dirname(os.path.abspath(__file__))
WORKER_ID = f"outbox-worker-{os.getpid()}"

# setup logging
if not os.path.exists(f"{directory}/logs"):
    os.makedirs(f"{directory}/logs", exist_ok=True)
logger = logging.getLogger('tl_outbox_worker')
if not logger.handlers:
    h = RotatingFileHandler(f"{directory}/logs/outbox_worker.log", maxBytes=5_000_000, backupCount=5, encoding='utf-8')
    h.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.setLevel(logging.INFO)
    logger.addHandler(h)

def lock_next_outbox(cs, lock_ttl=60):
    now = int(time.time())
    # attempt to atomically lock one new outbox row (parameterized)
    try:
        res = cs.execute(
            f"UPDATE {utl.outbox} SET status=%s, locked_by=%s, locked_until=%s WHERE status=%s AND (locked_until IS NULL OR locked_until < %s) LIMIT 1",
            ('locked', WORKER_ID, now + lock_ttl, 'new', now)
        )
        if res:
            cs.execute(f"SELECT * FROM {utl.outbox} WHERE locked_by=%s AND status=%s ORDER BY id DESC LIMIT 1", (WORKER_ID, 'locked'))
            row = cs.fetchone()
            try:
                logger.info('Locked outbox id=%s for sending (worker=%s)', row['id'], WORKER_ID)
            except Exception:
                pass
            return row
    except Exception:
        logger.exception('lock_next_outbox failed')
    return None

def send_row(cs, row, base_backoff=30, max_backoff=3600):
    try:
        # Fetch mbot info using parameterized query
        cs.execute(f"SELECT * FROM {utl.mbots} WHERE id=%s", (row['mbot_id'],))
        mbot = cs.fetchone()
        if mbot is None:
            cs.execute(f"UPDATE {utl.outbox} SET status=%s, last_error=%s, updated_at=%s WHERE id=%s", ('failed', 'mbot_not_found', int(time.time()), row['id']))
            return
        # If a listener process is active for this mbot it will create a lock file
        # sessions/{uniq_id}.lock. Respect that and postpone sending to avoid
        # concurrent Telethon session access (sqlite 'database is locked').
        try:
            lock_path = f"{directory}/sessions/{mbot['uniq_id']}.lock"
            if os.path.exists(lock_path):
                logger.info('Detected listener lock for mbot_id=%s (uniq=%s). Postponing outbox id=%s', mbot['id'], mbot.get('uniq_id'), row.get('id'))
                # reschedule with small backoff
                now = int(time.time())
                attempts = int(row.get('attempts', 0)) + 1
                backoff = min(base_backoff * (2 ** (attempts - 1)) + random.randint(0, base_backoff), max_backoff)
                cs.execute(f"UPDATE {utl.outbox} SET attempts=%s, last_error=%s, locked_until=%s, status=%s, updated_at=%s WHERE id=%s", (attempts, 'listener_lock', now + backoff, 'new', now, row['id']))
                return
        except Exception:
            # if anything goes wrong checking the lock, just proceed (we'll catch sqlite error)
            logger.exception('Error while checking listener lock file for mbot_id=%s', mbot['id'])
        # log detailed row info (sanitized)
        try:
            logger.info('Preparing to send outbox id=%s mbot_id=%s target_id=%s target_username=%s reply_to_inbox=%s attempts=%s', row.get('id'), row.get('mbot_id'), row.get('target_id'), row.get('target_username'), row.get('reply_to_inbox_id'), row.get('attempts'))
            logger.debug('Outbox row content: %s', {k: (v if k!='text' else (v[:200] + '...' if v and len(v)>200 else v)) for k,v in row.items()})
        except Exception:
            pass
        # To avoid races where multiple outbox workers try to open the same
        # Telethon sqlite session at the same time (causing sqlite3.OperationalError:
        # database is locked), attempt to atomically create a lock file for this
        # session before connecting. If the lock file already exists, postpone
        # this outbox row (another process/listener is using the session).
        lock_path = f"{directory}/sessions/{mbot['uniq_id']}.lock"
        created_lock_file = False
        try:
            # Try to create the lock file atomically. If it exists, os.open will fail.
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                os.write(fd, str(os.getpid()).encode('utf-8'))
            finally:
                os.close(fd)
            created_lock_file = True
            logger.info('Created temporary worker lock file for mbot_id=%s: %s', mbot['id'], lock_path)
        except FileExistsError:
            # Someone else (listener or another worker) holds the session lock.
            logger.info('Detected existing session lock for mbot_id=%s (uniq=%s). Postponing outbox id=%s', mbot['id'], mbot.get('uniq_id'), row.get('id'))
            now = int(time.time())
            attempts = int(row.get('attempts', 0)) + 1
            backoff = min(base_backoff * (2 ** (attempts - 1)) + random.randint(0, base_backoff), max_backoff)
            cs.execute(f"UPDATE {utl.outbox} SET attempts=%s, last_error=%s, locked_until=%s, status=%s, updated_at=%s WHERE id=%s", (attempts, 'session_locked', now + backoff, 'new', now, row['id']))
            return
        except Exception:
            # If creating the lock file failed for unexpected reasons, log and continue
            # — we'll still try to connect and handle sqlite errors via retries.
            logger.exception('Unexpected error while creating session lock file for mbot_id=%s', mbot['id'])

        client = telethon.sync.TelegramClient(session=f"{directory}/sessions/{mbot['uniq_id']}", api_id=mbot['api_id'], api_hash=mbot['api_hash'])
        logger.info('Outbox worker %s connecting with session=%s (mbot uniq=%s)', WORKER_ID, f"{directory}/sessions/{mbot['uniq_id']}", mbot.get('uniq_id'))
        # attempt to connect with simple exponential backoff to avoid abrupt failures
        conn_backoff = 1
        connected = False
        for _ in range(5):
            try:
                client.connect()
                connected = True
                break
            except Exception as e:
                logger.exception('client.connect failed for mbot_id=%s: %s', mbot['id'], e)
                sleep_time = min(conn_backoff, 60) + random.random()
                time.sleep(sleep_time)
                conn_backoff = min(conn_backoff * 2, 60)
        if not connected:
            # schedule row for retry with exponential backoff
            now = int(time.time())
            attempts = int(row.get('attempts', 0)) + 1
            max_attempts = int(row.get('max_attempts', 3))
            if attempts < max_attempts:
                backoff = min(base_backoff * (2 ** (attempts - 1)) + random.randint(0, base_backoff), max_backoff)
                cs.execute(f"UPDATE {utl.outbox} SET attempts=%s, last_error=%s, locked_until=%s, status=%s, updated_at=%s WHERE id=%s", (attempts, 'connect_failed', now + backoff, 'new', now, row['id']))
                logger.warning('Outbox id=%s connection failed, scheduled retry in %ss', row['id'], backoff)
            else:
                cs.execute(f"UPDATE {utl.outbox} SET attempts=%s, last_error=%s, status=%s, updated_at=%s WHERE id=%s", (attempts, 'connect_failed', 'failed', now, row['id']))
                logger.error('Outbox id=%s permanently failed due to connection errors', row['id'])
            return
        try:
            target = None
            if row.get('target_id'):
                try:
                    target = int(row['target_id'])
                except:
                    target = row['target_id']
            elif row.get('target_username'):
                target = row['target_username']

            # mark sending
            cs.execute(f"UPDATE {utl.outbox} SET status=%s, updated_at=%s WHERE id=%s", ('sending', int(time.time()), row['id']))
            logger.info('Marked outbox id=%s as sending', row['id'])
            if target is None:
                raise Exception('no_target')

            # determine reply_to message id (try explicit field, else lookup from inbox)
            reply_to_msg_id = None
            try:
                if row.get('reply_to_message_id'):
                    reply_to_msg_id = int(row.get('reply_to_message_id'))
            except Exception:
                reply_to_msg_id = None
            # if not provided, try to read the original incoming message id from inbox
            if not reply_to_msg_id and row.get('reply_to_inbox_id'):
                try:
                    cs.execute(f"SELECT * FROM {utl.inbox} WHERE id=%s", (int(row['reply_to_inbox_id']),))
                    inbox_row = cs.fetchone()
                    if inbox_row:
                        # try several common column names
                        for col in ('from_message_id', 'message_id', 'msg_id', 'incoming_id'):
                            try:
                                val = inbox_row.get(col)
                                if val:
                                    reply_to_msg_id = int(val)
                                    break
                            except Exception:
                                continue
                except Exception:
                    pass

            # send message and capture sent message id
            try:
                if reply_to_msg_id:
                    logger.info('Sending outbox id=%s to target=%s as reply_to=%s', row['id'], target, reply_to_msg_id)
                    sent_msg = client.send_message(entity=target, message=row['text'], reply_to=reply_to_msg_id)
                else:
                    logger.info('Sending outbox id=%s to target=%s without reply', row['id'], target)
                    sent_msg = client.send_message(entity=target, message=row['text'])
            except Exception:
                logger.exception('Failed to send message for outbox id=%s', row.get('id'))
                raise
            sent_id = getattr(sent_msg, 'id', None)

            # mark sent and store sent_message_id
            cs.execute(f"UPDATE {utl.outbox} SET status=%s, sent_message_id=%s, updated_at=%s WHERE id=%s", ('sent', sent_id, int(time.time()), row['id']))

            # if this outbox was a reply to an inbox record, link them
            if row.get('reply_to_inbox_id'):
                try:
                    cs.execute(f"UPDATE {utl.inbox} SET reply_to_outgoing_id=%s, processed=1 WHERE id=%s", (row['id'], int(row['reply_to_inbox_id'])))
                    # also store which message id we replied to (if available) into outbox/inbox if schema allows
                    if reply_to_msg_id:
                        try:
                            cs.execute(f"UPDATE {utl.outbox} SET reply_to_message_id=%s WHERE id=%s", (reply_to_msg_id, row['id']))
                        except Exception:
                            pass
                    logger.info('Linked outbox id=%s to inbox.id=%s and marked inbox processed', row['id'], row.get('reply_to_inbox_id'))
                except Exception:
                    pass
                # Notify admins via central bot that the message was delivered
                try:
                    notif = f"✅ پیام (outbox id={row['id']}) به <code>{target}</code> با شناسهٔ پیام ارسالی <code>{sent_id}</code> ارسال شد."
                    for admin_id in getattr(utl, 'admins', []):
                        try:
                            utl.bot.send_message(chat_id=admin_id, text=notif, parse_mode='HTML')
                        except Exception:
                            # don't let admin-notify failures affect main flow
                            logger.exception('Failed to notify admin %s about outbox id=%s', admin_id, row['id'])
                except Exception:
                    logger.exception('Unexpected error while notifying admins for outbox id=%s', row['id'])
        except FloodWaitError as fe:
            wait = int(getattr(fe, 'seconds', 60))
            cs.execute(f"UPDATE {utl.mbots} SET status=%s, end_restrict=%s WHERE id=%s", (2, int(time.time()) + wait, mbot['id']))
            cs.execute(f"UPDATE {utl.outbox} SET status=%s, last_error=%s, updated_at=%s WHERE id=%s", ('failed', f'FloodWait {wait}s', int(time.time()), row['id']))
        except Exception as e:
            # Generic error — increment attempts and schedule retry with exponential backoff if attempts < max_attempts
            try:
                attempts = int(row.get('attempts', 0))
            except:
                attempts = 0
            new_attempts = attempts + 1
            max_attempts = int(row.get('max_attempts', 3))
            err_msg = str(e)
            # compute backoff (seconds) with jitter to avoid thundering herd
            backoff = min(base_backoff * (2 ** attempts) + random.randint(0, base_backoff), max_backoff)
            now = int(time.time())
            if new_attempts < max_attempts:
                # requeue for retry after backoff
                cs.execute(f"UPDATE {utl.outbox} SET attempts=%s, last_error=%s, locked_until=%s, status=%s, updated_at=%s WHERE id=%s", (new_attempts, err_msg, now + backoff, 'new', now, row['id']))
                try:
                    logger.warning('Outbox id=%s transient error, attempts=%s scheduled retry in %ss: %s', row['id'], new_attempts, backoff, err_msg)
                except Exception:
                    pass
            else:
                # mark as failed permanently
                cs.execute(f"UPDATE {utl.outbox} SET attempts=%s, last_error=%s, status=%s, updated_at=%s WHERE id=%s", (new_attempts, err_msg, 'failed', now, row['id']))
                try:
                    logger.error('Outbox id=%s permanently failed after %s attempts: %s', row['id'], new_attempts, err_msg)
                except Exception:
                    pass
        finally:
            try:
                client.disconnect()
            except:
                pass
            # remove temporary worker lock file if we created it
            try:
                if created_lock_file:
                    if os.path.exists(lock_path):
                        try:
                            os.remove(lock_path)
                            logger.info('Removed temporary worker lock file: %s', lock_path)
                        except Exception:
                            logger.exception('Failed to remove temporary worker lock file: %s', lock_path)
            except Exception:
                pass
    except Exception:
        logger.exception('Unexpected error in send_row')

def send_loop():
    cs = utl.Database().data()
    while True:
        try:
            row = lock_next_outbox(cs)
            if not row:
                time.sleep(1)
                continue
            send_row(cs, row)
        except Exception:
            traceback.print_exc()
            time.sleep(2)

if __name__ == '__main__':
    send_loop()
