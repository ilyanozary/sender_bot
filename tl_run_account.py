import os, re, sys, time, datetime, telethon, telethon.sync, utility as utl


for index, arg in enumerate(sys.argv):
    if index == 1:
        mbots_uniq_id = arg
    elif index == 2:
        order_id = int(arg)

directory = os.path.dirname(os.path.abspath(__file__))
filename = str(os.path.basename(__file__))

cs = utl.Database()
cs = cs.data()

cs.execute(f"SELECT * FROM {utl.admin}")
row_admin = cs.fetchone()
cs.execute(f"SELECT * FROM {utl.orders} WHERE id={order_id}")
row_orders = cs.fetchone()
cs.execute(f"SELECT * FROM {utl.mbots} WHERE uniq_id='{mbots_uniq_id}'")
row_mbots = cs.fetchone()

utl.get_params_pids_by_full_script_name(script_names=[f"{directory}/{filename}"], param1=row_mbots["uniq_id"], is_kill_proccess=True)


def check_report(client):
    try:
        for r in client(telethon.functions.messages.StartBotRequest(bot="@spambot", peer="@spambot", start_param="start")).updates:
            time.sleep(1)
            for r1 in client(telethon.functions.messages.GetMessagesRequest(id=[r.id + 1])).messages:
                if "I’m afraid some Telegram users found your messages annoying and forwarded them to our team of moderators for inspection." in r1.message:
                    if "Unfortunately, your account is now limited" in r1.message:
                        return int(time.time()) + 604800
                    else:
                        regex = re.findall('automatically released on [\d\w ,:]*UTC', r1.message)[0]
                        date_str = regex.replace("automatically released on ","")
                        utc_time = datetime.datetime.strptime(date_str, '%d %b %Y, %H:%M %Z')
                        timestamp = utc_time.replace(tzinfo=datetime.timezone.utc).timestamp()
                        return int(timestamp)
                elif "While the account is limited, you will not be able to send messages to people who do not have your number in their phone contacts" in r1.message:
                    return int(time.time()) + 604800
            break
    except:
        pass
    return False


def operation(cs, row_orders, row_mbots, result):
    try:
        count_send = i = 0
        cs.execute(f"SELECT COUNT(*) as count FROM {utl.reports} WHERE order_id={row_orders['id']} AND status=1")
        total_send = cs.fetchone()['count']
        if total_send > 0 and total_send >= row_orders['count']:
            return utl.end_order(cs, f"{directory}/files/exo_{row_orders['id']}_r.txt", row_orders)
        
        # حساب‌کردن اکانت‌های استفاده‌شده و ثبت usedaccs را بعد از احراز هویت و بررسی محدودیت انجام می‌دهیم
        
        client = telethon.sync.TelegramClient(session=f"{directory}/sessions/{row_mbots['uniq_id']}", api_id=row_mbots['api_id'], api_hash=row_mbots['api_hash'])
        client.connect()
        if not client.is_user_authorized():
            cs.execute(f"UPDATE {utl.mbots} SET status=0 WHERE id={row_mbots['id']}")
            cs.execute(f"UPDATE {utl.orders} SET count_accout=count_accout+1 WHERE id={row_orders['id']}")
            return print(f"{row_mbots['id']}: Log Out")
        
        restrict = check_report(client)
        if restrict:
            cs.execute(f"UPDATE {utl.mbots} SET status=2,end_restrict={restrict} WHERE id={row_mbots['id']}")
            cs.execute(f"UPDATE {utl.orders} SET count_report=count_report+1 WHERE id={row_orders['id']}")
            return print(f"{row_mbots['id']}: Limited")

        # فقط اگر اکانت احراز هویت شده و محدود نیست، به‌عنوان استفاده‌شده ثبت می‌کنیم
        cs.execute(f"INSERT INTO {utl.usedaccs} (order_id,bot_id,created_at) VALUES ({row_orders['id']},{row_mbots['id']},{int(time.time())})")
        cs.execute(f"UPDATE {utl.mbots} SET last_order_at={int(time.time())} WHERE id={row_mbots['id']}")
        cs.execute(f"UPDATE {utl.orders} SET count_acc=count_acc+1,updated_at={int(time.time())} WHERE id={row_orders['id']}")
        
        limit_per_h = int(time.time()) + row_admin['limit_per_h']
        cs.execute(f"UPDATE {utl.mbots} SET status=2,end_restrict={limit_per_h} WHERE id={row_mbots['id']}")
        # Process sends batched by `batch` and ordered by `msg_index`
        while count_send < row_orders['send_per_h']:
            # pick the smallest batch that actually has message templates
            cs.execute(
                f"SELECT MIN(a.batch) as batch FROM {utl.analyze} a "
                f"WHERE a.order_id=%s AND EXISTS (SELECT 1 FROM {utl.files} f WHERE f.order_id=a.order_id AND f.batch=a.batch)",
                (row_orders['id'],)
            )
            row_min = cs.fetchone()
            current_batch = row_min['batch'] if row_min and row_min.get('batch') is not None else None
            if current_batch is None:
                break
            current_batch = int(current_batch)

            msgs = []
            result_batch = []
            cs.execute(
                f"SELECT * FROM {utl.files} WHERE order_id=%s AND batch=%s ORDER BY msg_index ASC",
                (row_orders['id'], current_batch)
            )
            result_plus = cs.fetchall()
            for row_pus in result_plus:
                # Fetch the message stored in cache channel by id. Normalize to a single Message object.
                try:
                    fetched = client.get_messages(f"@{row_admin['cache']}", ids=row_pus['message_id'])
                except Exception as e:
                    print(f"{row_mbots['id']}: Failed to fetch template message id={row_pus['message_id']}: {e}")
                    fetched = None
                if isinstance(fetched, list):
                    if len(fetched) > 0:
                        msgs.append(fetched[0])
                elif fetched is not None:
                    msgs.append(fetched)
            # If this batch has no available template messages, skip and try next batch
            if not msgs:
                print(f"{row_mbots['id']}: batch {current_batch} has no template messages; skipping")
                # mark a small sleep to avoid tight loop; next iteration will select next eligible batch
                time.sleep(0.3)
                continue
            # Use MySQL advisory lock so only one account computes reservation at a time
            lock_key = f"pv_order_{row_orders['id']}_batch_{current_batch}"
            cs.execute("SELECT GET_LOCK(%s, 5) AS ok", (lock_key,))
            got_lock_row = cs.fetchone() or {"ok": 0}
            got_lock = int(got_lock_row.get("ok", 0)) == 1
            if not got_lock:
                # someone else is reserving; wait briefly and retry loop
                time.sleep(0.3)
                continue
            try:
                # how many targets are currently unreserved in this batch
                cs.execute(
                    f"SELECT COUNT(*) as c FROM {utl.analyze} WHERE order_id=%s AND batch=%s AND reserved_by IS NULL",
                    (row_orders['id'], current_batch)
                )
                unreserved_batch = int((cs.fetchone() or {"c": 0}).get("c", 0))
                # already reserved by any account (informational)
                cs.execute(
                    f"SELECT COUNT(*) as c FROM {utl.analyze} WHERE order_id=%s AND batch=%s AND reserved_by IS NOT NULL",
                    (row_orders['id'], current_batch)
                )
                reserved_any = int((cs.fetchone() or {"c": 0}).get("c", 0))
                remaining_global = unreserved_batch
                remaining_account = int(row_orders['send_per_h']) - count_send
                remaining = remaining_global if remaining_global < remaining_account else remaining_account
                print(f"{row_mbots['id']}: lock ok batch={current_batch}, unreserved={unreserved_batch}, reserved_any={reserved_any}, rem_global={remaining_global}, rem_acc={remaining_account}, will_reserve={remaining}")
                if remaining <= 0:
                    # nothing left globally for this batch
                    cs.execute("SELECT RELEASE_LOCK(%s)", (lock_key,))
                    break

                # Transactional reservation to prevent two accounts reserving same rows
                reserve_tag = f"mbot-{row_mbots['id']}"
                try:
                    cs.execute("BEGIN")
                    cs.execute(
                        f"SELECT id FROM {utl.analyze} WHERE order_id=%s AND batch=%s AND reserved_by IS NULL ORDER BY id ASC LIMIT %s FOR UPDATE",
                        (row_orders['id'], current_batch, remaining)
                    )
                    ids = [r['id'] for r in (cs.fetchall() or [])]
                    if ids:
                        in_ids = ','.join(str(i) for i in ids)
                        cs.execute(f"UPDATE {utl.analyze} SET reserved_by=%s WHERE id IN ({in_ids})", (reserve_tag,))
                        print(f"{row_mbots['id']}: reserved ids: {ids}")
                    cs.execute("COMMIT")
                except Exception:
                    # best-effort non-locking reservation fallback
                    try:
                        cs.execute(
                            f"UPDATE {utl.analyze} SET reserved_by=%s WHERE order_id=%s AND batch=%s AND reserved_by IS NULL LIMIT %s",
                            (reserve_tag, row_orders['id'], current_batch, remaining)
                        )
                        print(f"{row_mbots['id']}: fallback reservation attempted for {remaining} rows")
                    except Exception:
                        pass
            finally:
                # always release advisory lock
                try:
                    cs.execute("SELECT RELEASE_LOCK(%s)", (lock_key,))
                except Exception:
                    pass

            cs.execute(
                f"SELECT * FROM {utl.analyze} WHERE order_id=%s AND batch=%s AND reserved_by=%s ORDER BY id ASC LIMIT %s",
                (row_orders['id'], current_batch, reserve_tag, remaining)
            )
            result_batch = cs.fetchall()
            if not result_batch:
                print(f"{row_mbots['id']}: no rows reserved; breaking loop")
                break

            for row in result_batch:
                cs.execute(f"UPDATE {utl.orders} SET count_request=count_request+1 WHERE id={row_orders['id']}")
                try:
                    # choose a target entity (username or numeric id)
                    target = row.get('username') if row.get('username') else row.get('user_id')
                    if not target:
                        raise Exception('No target username or user_id')

                    if not msgs:
                        # No template messages were fetched for this batch — do not mark as sent
                        raise Exception('No template messages available')

                    sent_ok = False
                    for message in msgs:
                        # normalize message content
                        text_content = getattr(message, 'message', None)
                        has_media = getattr(message, 'media', None) is not None
                        if not has_media:
                            # send text message (use text_content or fallback to empty string)
                            client.send_message(entity=target, message=(text_content or ''), parse_mode='html')
                        else:
                            # send media with caption
                            # Telethon accepts message.media or the Message object itself for send_file
                            client.send_file(entity=target, file=message.media if getattr(message, 'media', None) is not None else message, caption=(text_content or ''), parse_mode='html')
                        sent_ok = True

                    if sent_ok:
                        # only remove from analyze and record report after successful send
                        try:
                            cs.execute(f"DELETE FROM {utl.analyze} WHERE id=%s", (row['id'],))
                        except Exception:
                            cs.execute(f"DELETE FROM {utl.analyze} WHERE order_id=%s AND username=%s", (row_orders['id'], row['username']))
                        cs.execute(f"UPDATE {utl.orders} SET count_done=count_done+1 WHERE id={row_orders['id']}")
                        # insert report record (parameterized to avoid quoting issues)
                        cs.execute(
                            f"INSERT INTO {utl.reports} (order_id,bot_id,user_id,username,group_id,status,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                            (row_orders['id'], row_mbots['id'], row['user_id'], row['username'], row_orders.get('group_id', None), 1, int(time.time()))
                        )
                        print(f"{row_mbots['id']} ({i}): Send")
                        count_send += 1
                        if (total_send + count_send) >= row_orders['count']:
                            return
                        if count_send >= row_orders['send_per_h']:
                            return
                except telethon.errors.FloodWaitError as e:
                    print(f"{row_mbots['id']} ({i}): Restricted when Send")
                    end_restrict = int(time.time()) + int(e.seconds)
                    if end_restrict > limit_per_h:
                        cs.execute(f"UPDATE {utl.mbots} SET status=2,end_restrict={end_restrict} WHERE id={row_mbots['id']}")
                    return cs.execute(f"UPDATE {utl.orders} SET count_restrict=count_restrict+1,count_restrict_error=count_restrict_error+1 WHERE id={row_orders['id']}")
                except Exception as e:
                    error = str(e)
                    print(f"{row_mbots['id']} ({i}): Error when Send: {e}")
                    if 'Too many requests' in error:
                        cs.execute(f"UPDATE {utl.orders} SET count_usrspam=count_usrspam+1 WHERE id={row_orders['id']}")
                    elif 'No user has' in error or 'The specified user was deleted' in error or 'ResolveUsernameRequest' in error:
                        cs.execute(f"UPDATE {utl.orders} SET count_userincorrect=count_userincorrect+1 WHERE id={row_orders['id']}")
                        # remove all analyze rows for this username (they are invalid/non-existent)
                        try:
                            cs.execute(f"DELETE FROM {utl.analyze} WHERE order_id=%s AND username=%s", (row_orders['id'], row.get('username')))
                            print(f"{row_mbots['id']}: removed invalid username from analyze: {row.get('username')}")
                        except Exception:
                            pass
                    elif 'You can\'t write in this chat' in error:
                        cs.execute(f"UPDATE {utl.orders} SET count_restrict_error=count_restrict_error+1 WHERE id={row_orders['id']}")
                    else:
                        cs.execute(f"UPDATE {utl.orders} SET count_other_errors=count_other_errors+1 WHERE id={row_orders['id']}")
                i += 1
                if i % 3 == 0:
                    time.sleep(1)
    except Exception as e:
        print(f"{row_mbots['id']}: Error when Start: {e}")
    finally:
        try:
            client.disconnect()
        except:
            pass
    print(f"{row_mbots['id']}: RESULT: [{count_send} / {total_send}]")


if row_orders is not None and row_mbots is not None:
    cs.execute(f"SELECT * FROM {utl.analyze} LIMIT {row_orders['send_per_h']}")
    result_analyze = cs.fetchall()
    if result_analyze:
        operation(cs, row_orders, row_mbots, result_analyze)
    else:
        utl.end_order(cs, f"{directory}/files/exo_{row_orders['id']}_r.txt", row_orders)
    