import os, sys, time, traceback, telethon, telethon.sync, utility as utl, random
from telethon import events
import logging
from logging.handlers import RotatingFileHandler
import asyncio

directory = os.path.dirname(os.path.abspath(__file__))

# logging
if not os.path.exists(f"{directory}/logs"):
    os.makedirs(f"{directory}/logs", exist_ok=True)
logger = logging.getLogger('tl_inbox_listener')
if not logger.handlers:
    h = RotatingFileHandler(f"{directory}/logs/inbox_listener.log", maxBytes=2_000_000, backupCount=3, encoding='utf-8')
    h.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.setLevel(logging.INFO)
    logger.addHandler(h)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python3 tl_inbox_listener.py <mbots_uniq_id>')
        sys.exit(1)
    mbots_uniq_id = sys.argv[1]

cs = utl.Database().data()
cs.execute(f"SELECT * FROM {utl.mbots} WHERE uniq_id='{mbots_uniq_id}'")
row_mbots = cs.fetchone()
if row_mbots is None:
    print('mbot not found')
    sys.exit(1)

client = telethon.sync.TelegramClient(session=f"{directory}/sessions/{row_mbots['uniq_id']}", api_id=row_mbots['api_id'], api_hash=row_mbots['api_hash'])
try:
    client.connect()
    logger.info('Connected Telethon client for mbot uniq_id=%s (mbot id=%s)', row_mbots['uniq_id'], row_mbots['id'])
    # create a listener lock file to signal other processes (workers) that this
    # session is in-use by the listener. The lock file contains the pid.
    try:
        lock_path = f"{directory}/sessions/{row_mbots['uniq_id']}.lock"
        with open(lock_path, 'w', encoding='utf-8') as f:
            f.write(str(os.getpid()))
        logger.info('Created listener lock file: %s', lock_path)
    except Exception:
        logger.exception('Failed to create listener lock file')
except Exception as e:
    logger.exception('Failed to connect client: %s', e)
    print('Failed to connect client:', e)
    sys.exit(1)

cs = utl.Database().data()

# start a background task inside this listener process to send outbox messages
async def listener_outbox_sender_loop():
    """Simpler polling sender: select one new outbox row for this mbot, attempt to
    atomically claim it with an UPDATE by id, then send using the same client.
    This reduces nested try/except complexity and prevents sqlite locking from
    multiple processes opening same session file."""
    listener_lock_id = f"listener-{row_mbots['id']}"
    while True:
        try:
            cs2 = utl.Database().data()
            now = int(time.time())
            # pick one candidate row
            try:
                cs2.execute(f"SELECT * FROM {utl.outbox} WHERE status=%s AND mbot_id=%s ORDER BY id ASC LIMIT 1", ('new', row_mbots['id']))
                out_row = cs2.fetchone()
            except Exception:
                out_row = None

            if not out_row:
                await asyncio.sleep(0.8)
                continue

            # try to claim it by id (avoid race)
            try:
                res = cs2.execute(f"UPDATE {utl.outbox} SET status=%s, locked_by=%s, locked_until=%s WHERE id=%s AND status=%s", ('locked', listener_lock_id, now + 60, out_row['id'], 'new'))
            except Exception:
                res = 0

            if not res:
                # someone else grabbed it; try next iteration
                await asyncio.sleep(0.5)
                continue

            # determine target and reply_to
            target = None
            if out_row.get('target_id'):
                try:
                    target = int(out_row.get('target_id'))
                except:
                    target = out_row.get('target_id')
            elif out_row.get('target_username'):
                target = out_row.get('target_username')

            reply_to_msg_id = None
            try:
                if out_row.get('reply_to_message_id'):
                    reply_to_msg_id = int(out_row.get('reply_to_message_id'))
            except Exception:
                reply_to_msg_id = None
            if not reply_to_msg_id and out_row.get('reply_to_inbox_id'):
                try:
                    cs2.execute(f"SELECT * FROM {utl.inbox} WHERE id=%s", (int(out_row['reply_to_inbox_id']),))
                    inbox_row = cs2.fetchone()
                    if inbox_row:
                        for col in ('from_message_id', 'message_id', 'msg_id', 'incoming_id'):
                            try:
                                v = inbox_row.get(col)
                                if v:
                                    reply_to_msg_id = int(v)
                                    break
                            except Exception:
                                continue
                except Exception:
                    pass

            # preflight: try to mark the incoming message(s) as read so the
            # recipient sees the message as replied-to (reduces ambiguity).
            # Use Telethon high-level method send_read_acknowledge which wraps
            # the correct TL call. Older direct ReadHistoryRequest signatures
            # differ across Telethon versions and caused TypeError in logs.
            try:
                if reply_to_msg_id and target is not None:
                    try:
                        await client.send_read_acknowledge(target, max_id=reply_to_msg_id)
                        logger.info('Marked messages read up to %s for preflight outbox id=%s target=%s', reply_to_msg_id, out_row.get('id'), target)
                    except Exception as e_mark:
                        logger.exception('Failed to mark messages read for preflight outbox id=%s: %s', out_row.get('id'), e_mark)
            except Exception:
                # defensive: do not let marking errors block sending
                logger.exception('Unexpected error in preflight mark-read for outbox id=%s', out_row.get('id'))

            # send with client (use await since we're in async context)
            try:
                if reply_to_msg_id:
                    sent = await client.send_message(entity=target, message=out_row.get('text'), reply_to=reply_to_msg_id)
                else:
                    sent = await client.send_message(entity=target, message=out_row.get('text'))
                sent_id = getattr(sent, 'id', None)
                cs2.execute(f"UPDATE {utl.outbox} SET status=%s, sent_message_id=%s, updated_at=%s WHERE id=%s", ('sent', sent_id, int(time.time()), out_row['id']))
                if out_row.get('reply_to_inbox_id'):
                    try:
                        cs2.execute(f"UPDATE {utl.inbox} SET reply_to_outgoing_id=%s, processed=1 WHERE id=%s", (out_row['id'], int(out_row['reply_to_inbox_id'])))
                    except Exception:
                        pass
                logger.info('Listener-sent outbox id=%s to target=%s reply_to=%s sent_id=%s', out_row['id'], target, reply_to_msg_id, sent_id)
                # Notify admins via central bot that the message was delivered
                try:
                    notif = f"✅ پیام (outbox id={out_row['id']}) به <code>{target}</code> با شناسهٔ پیام ارسالی <code>{sent_id}</code> ارسال شد."
                    for admin_id in getattr(utl, 'admins', []):
                        try:
                            utl.bot.send_message(chat_id=admin_id, text=notif, parse_mode='HTML')
                        except Exception:
                            logger.exception('Failed to notify admin %s about outbox id=%s', admin_id, out_row['id'])
                except Exception:
                    logger.exception('Unexpected error while notifying admins for outbox id=%s', out_row['id'])
            except Exception as e_send:
                logger.exception('Listener failed to send outbox id=%s: %s', out_row.get('id'), e_send)
                # retry/backoff
                try:
                    attempts = int(out_row.get('attempts', 0)) + 1
                except:
                    attempts = 1
                max_attempts = int(out_row.get('max_attempts', 3)) if out_row.get('max_attempts') is not None else 3
                now2 = int(time.time())
                if attempts < max_attempts:
                    backoff = min(30 * (2 ** (attempts - 1)), 3600)
                    sql_up = f"UPDATE {utl.outbox} SET attempts=%s, last_error=%s, locked_until=%s, status=%s, updated_at=%s WHERE id=%s"
                    params_up = (attempts, str(e_send), now2 + backoff, 'new', now2, out_row['id'])
                    cs2.execute(sql_up, params_up)
                else:
                    sql_fail = f"UPDATE {utl.outbox} SET attempts=%s, last_error=%s, status=%s, updated_at=%s WHERE id=%s"
                    params_fail = (attempts, str(e_send), 'failed', now2, out_row['id'])
                    cs2.execute(sql_fail, params_fail)
        except Exception:
            logger.exception('Unexpected error in listener_outbox_sender_loop')
            await asyncio.sleep(2)

try:
    # schedule the outbox sender loop on the client's event loop
    client.loop.create_task(listener_outbox_sender_loop())
    logger.info('Started listener_outbox_sender_loop for mbot id=%s', row_mbots['id'])
except Exception:
    logger.exception('Failed to start listener_outbox_sender_loop')

@client.on(events.NewMessage(incoming=True))
async def handler(event):
    try:
        msg = event.message
        # log basic incoming information for debugging
        try:
            incoming_msg_id = getattr(msg, 'id', None) or getattr(msg, 'message_id', None)
        except Exception:
            incoming_msg_id = None
        try:
            sender_preview = await event.get_sender()
            sp_id = getattr(sender_preview, 'id', None)
            sp_un = getattr(sender_preview, 'username', None)
        except Exception:
            sp_id = None
            sp_un = None
        try:
            logger.info('NewMessage received: mbot_id=%s mbot_uniq=%s msg_id=%s sender_id=%s sender_username=%s', row_mbots['id'], row_mbots.get('uniq_id'), incoming_msg_id, sp_id, sp_un)
        except Exception:
            pass

        # if global inbox listening is disabled via admin settings, skip processing (log this event)
        try:
            cs.execute(f"SELECT disable_inbox FROM {utl.admin}")
            admin_row = cs.fetchone()
            if admin_row and int(admin_row.get('disable_inbox', 0)) == 1:
                try:
                    logger.info('Global inbox listening is disabled (admin.disable_inbox=1) — ignoring incoming message mbot_id=%s msg_id=%s', row_mbots['id'], incoming_msg_id)
                except Exception:
                    pass
                return
        except Exception as e:
            # log DB read error but continue processing
            logger.exception('Error reading admin.disable_inbox: %s', e)
        # normalize sender identification: prefer sender object if available
        from_id = None
        from_username = None
        sender = await event.get_sender()
        if sender is not None:
            # Telethon sender has .id and possibly .username
            try:
                if getattr(sender, 'id', None):
                    from_id = int(sender.id)
            except Exception:
                from_id = None
            try:
                if getattr(sender, 'username', None):
                    from_username = sender.username
            except Exception:
                from_username = None
            # capture first and last name when available
            try:
                from_first_name = getattr(sender, 'first_name', None)
            except Exception:
                from_first_name = None
            try:
                from_last_name = getattr(sender, 'last_name', None)
            except Exception:
                from_last_name = None
        # fallback to msg.from_id if sender info not available
        if from_id is None:
            if msg.from_id and hasattr(msg.from_id, 'user_id'):
                from_id = msg.from_id.user_id
            elif msg.from_id:
                try:
                    from_id = int(msg.from_id)
                except:
                    from_id = None

        # determine text: prefer message text, otherwise detect media type and use a short label
        text = ''
        if getattr(msg, 'message', None) and str(msg.message).strip():
            text = msg.message
        else:
            # detect media type robustly using Telethon media/document attributes
            try:
                media_label = None
                # photo attribute
                if getattr(msg, 'photo', None):
                    media_label = 'عکس'
                else:
                    media = getattr(msg, 'media', None)
                    doc = None
                    if media is not None:
                        # Telethon media may carry a document
                        doc = getattr(media, 'document', None) or getattr(msg, 'document', None)
                    # stickers / animations may be documents with attributes
                    if doc is not None:
                        try:
                            from telethon.tl import types as tltypes
                            attrs = getattr(doc, 'attributes', []) or []
                            is_animated = any(isinstance(a, tltypes.DocumentAttributeAnimated) for a in attrs)
                            is_sticker = any(isinstance(a, tltypes.DocumentAttributeSticker) for a in attrs)
                            is_video_attr = any(isinstance(a, tltypes.DocumentAttributeVideo) for a in attrs)
                            is_audio_attr = any(isinstance(a, tltypes.DocumentAttributeAudio) for a in attrs)
                            if is_sticker:
                                media_label = 'استیکر'
                            elif is_animated:
                                media_label = 'گیف'
                            elif is_video_attr:
                                media_label = 'ویدئو'
                            elif is_audio_attr:
                                # DocumentAttributeAudio includes voice/instant audio; try to disambiguate via mime
                                mime = getattr(doc, 'mime_type', '') or ''
                                if 'ogg' in mime or 'opus' in mime or 'voice' in mime:
                                    media_label = 'ویس'
                                else:
                                    media_label = 'صوت'
                            else:
                                # fallback to mime-type
                                mime = getattr(doc, 'mime_type', '') or ''
                                if mime.startswith('image/'):
                                    media_label = 'عکس'
                                elif mime.startswith('video/'):
                                    media_label = 'ویدئو'
                                elif mime.startswith('audio/'):
                                    # further check for voice by filename/codec
                                    if 'ogg' in mime or 'opus' in mime:
                                        media_label = 'ویس'
                                    else:
                                        media_label = 'صوت'
                                else:
                                    media_label = 'فایل'
                        except Exception:
                            media_label = 'فایل'
                    else:
                        # other explicit attributes on Message
                        if getattr(msg, 'sticker', None):
                            media_label = 'استیکر'
                        elif getattr(msg, 'animation', None):
                            media_label = 'گیف'
                        elif getattr(msg, 'voice', None):
                            media_label = 'ویس'
                        elif getattr(msg, 'video_note', None):
                            media_label = 'ویدئونوت'
                        elif getattr(msg, 'video', None):
                            media_label = 'ویدئو'
                        elif getattr(msg, 'audio', None):
                            media_label = 'صوت'
                if media_label:
                    text = f"({media_label})"
                else:
                    text = '(بدون متن)'
            except Exception:
                text = '(بدون متن)'

        # keep threads grouped by sender id when possible
        thread_id = str(from_id) if from_id is not None else utl.unique_id()

        # check if this sender is blocked for this mbot; if so, skip inserting
        try:
            is_blocked = False
            # prefer numeric id check when available
            if from_id is not None:
                try:
                    cs.execute(f"SELECT COUNT(*) as cnt FROM {utl.inbox_blocked} WHERE mbot_id=%s AND from_id=%s", (row_mbots['id'], from_id))
                    brow = cs.fetchone()
                    if brow and int(brow.get('cnt', 0)) > 0:
                        is_blocked = True
                except Exception:
                    # ignore and continue to username check
                    pass
            # check username (strip leading @)
            if (not is_blocked) and from_username:
                try:
                    uname_check = from_username.lstrip('@') if isinstance(from_username, str) else from_username
                    cs.execute(f"SELECT COUNT(*) as cnt FROM {utl.inbox_blocked} WHERE mbot_id=%s AND from_username=%s", (row_mbots['id'], uname_check))
                    brow2 = cs.fetchone()
                    if brow2 and int(brow2.get('cnt', 0)) > 0:
                        is_blocked = True
                except Exception:
                    pass

            if is_blocked:
                try:
                    logger.info('Skipping inbox insert for blocked sender: mbot_id=%s from_id=%s from_username=%s', row_mbots['id'], from_id, from_username)
                except Exception:
                    pass
                return
        except Exception:
            # If the block-check itself fails, log and continue (fail-open)
            try:
                logger.exception('Error while checking inbox_blocked table')
            except Exception:
                pass
        reply_to_outgoing_id = None
        created_at = int(time.time())
        try:
            # Parameterized insert to avoid SQL injection and handle NULLs
            # include first/last name if available
            sql = f"INSERT INTO {utl.inbox} (mbot_id,from_id,from_username,from_first_name,from_last_name,text,reply_to_outgoing_id,thread_id,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            params = (
                row_mbots['id'],
                from_id,
                from_username,
                from_first_name if 'from_first_name' in locals() else None,
                from_last_name if 'from_last_name' in locals() else None,
                text,
                reply_to_outgoing_id,
                thread_id,
                created_at
            )
            cs.execute(sql, params)
            # capture the inserted inbox id if available
            try:
                inserted_id = getattr(cs, 'lastrowid', None)
            except Exception:
                inserted_id = None
            # try to store incoming message id into inbox row if column exists; log outcome
            try:
                incoming_msg_id = getattr(msg, 'id', None) or getattr(msg, 'message_id', None)
                if incoming_msg_id is not None and inserted_id is not None:
                    try:
                        cs.execute(f"UPDATE {utl.inbox} SET from_message_id=%s WHERE id=%s", (int(incoming_msg_id), inserted_id))
                        logger.info('Updated inbox.id=%s with from_message_id=%s', inserted_id, incoming_msg_id)
                    except Exception as e_up:
                        # fallback try other common column names and log the exception
                        logger.warning('Failed to UPDATE from_message_id for inbox.id=%s: %s — trying alternate column names', inserted_id, e_up)
                        try:
                            cs.execute(f"UPDATE {utl.inbox} SET message_id=%s WHERE id=%s", (int(incoming_msg_id), inserted_id))
                            logger.info('Updated inbox.id=%s with message_id=%s', inserted_id, incoming_msg_id)
                        except Exception as e_up2:
                            logger.exception('Failed to UPDATE message id for inbox.id=%s: %s', inserted_id, e_up2)
            except Exception as e:
                logger.exception('Error while attempting to store incoming message id into inbox row: %s', e)
            try:
                logger.info('Inserted inbox row: id=%s mbot_id=%s from_id=%s from_username=%s len_text=%s thread=%s', inserted_id, row_mbots['id'], from_id, from_username, (len(text) if text else 0), thread_id)
            except Exception:
                pass
            # Notify admins via central bot about the new message (short notification with actions)
            try:
                try:
                    # prepare a short snippet for the notification (2 words or 40 chars)
                    raw = text if text else ''
                    words = raw.strip().split()
                    if len(words) >= 2:
                        snippet = ' '.join(words[:2])
                    else:
                        snippet = raw.strip()[:40]
                    snippet = snippet.replace('\n', ' ').replace('\r', '')
                    if not snippet:
                        snippet = '(بدون متن)'
                except Exception:
                    snippet = '(بدون متن)'
                mb_phone = row_mbots.get('phone') if row_mbots is not None else ''
                sender_label = from_username if from_username else (str(from_id) if from_id is not None else '')
                notif_text = f"🔔 اکانت <code>{mb_phone}</code> پیام جدید از <b>{sender_label}</b> دارد"
                # build inline keyboard: reply and view full
                kb = [
                    [
                        {'text': 'پاسخ به پیام', 'callback_data': f'inbox_select;{inserted_id if inserted_id is not None else 0};{row_mbots["id"]}'},
                        {'text': 'مشاهده کامل پیام', 'callback_data': f'inbox_more;{inserted_id if inserted_id is not None else 0};{row_mbots["id"]};new;{thread_id}'}
                    ]
                ]
                # send to all admins from config (utl.admins)
                for admin_id in getattr(utl, 'admins', []):
                    try:
                        utl.bot.send_message(chat_id=admin_id, text=notif_text, parse_mode='HTML', disable_web_page_preview=True, reply_markup={'inline_keyboard': kb})
                    except Exception:
                        # ignore per-admin send errors
                        pass
            except Exception:
                logger.exception('Failed to send admin notification for inbox row')
        except Exception:
            logger.exception('Failed to insert inbox row')
    except Exception:
        traceback.print_exc()

# Keep the client running and reconnect on unexpected errors with backoff
backoff = 1
try:
    while True:
        try:
            client.run_until_disconnected()
            # If run_until_disconnected returns normally, wait a bit and try to reconnect
            logger.info('run_until_disconnected returned, reconnecting in 5s')
            time.sleep(5)
            try:
                client.connect()
                logger.info('Reconnected client for mbot uniq_id=%s', row_mbots['uniq_id'])
                backoff = 1
            except Exception as e:
                logger.exception('Reconnect attempt failed: %s', e)
                backoff = min(backoff * 2, 60)
                time.sleep(backoff + random.random())
                continue
        except KeyboardInterrupt:
            logger.info('KeyboardInterrupt received, disconnecting client')
            try:
                client.disconnect()
            except:
                pass
            break
        except Exception as e:
            logger.exception('Client disconnected with error: %s', e)
            try:
                client.disconnect()
            except:
                pass
            sleep_time = min(backoff, 60) + random.random()
            logger.info('Reconnecting after %.1f seconds', sleep_time)
            time.sleep(sleep_time)
            try:
                client.connect()
                logger.info('Reconnected client for mbot uniq_id=%s', row_mbots['uniq_id'])
                backoff = 1
            except Exception as e2:
                logger.exception('Reconnect failed: %s', e2)
                backoff = min(backoff * 2, 60)
                continue
finally:
    try:
        client.disconnect()
    except:
        pass
    # remove lock file on exit
    try:
        lock_path = f"{directory}/sessions/{row_mbots['uniq_id']}.lock"
        if os.path.exists(lock_path):
            try:
                os.remove(lock_path)
                logger.info('Removed listener lock file: %s', lock_path)
            except Exception:
                logger.exception('Failed to remove listener lock file: %s', lock_path)
    except Exception:
        pass
