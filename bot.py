import os, re, time, shutil, requests, zipfile, datetime, jdatetime, telegram, telegram.ext, utility as utl, subprocess, logging
from logging.handlers import RotatingFileHandler


directory = os.path.dirname(os.path.abspath(__file__))
filename = str(os.path.basename(__file__))

# ensure logs dir
if not os.path.exists(f"{directory}/logs"):
    os.makedirs(f"{directory}/logs", exist_ok=True)
# setup logger for bot actions
logger = logging.getLogger('central_bot')
if not logger.handlers:
    handler = RotatingFileHandler(f"{directory}/logs/bot.log", maxBytes=2_000_000, backupCount=3, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

utl.get_params_pids_by_full_script_name(script_names=[f"{directory}/{filename}"], is_kill_proccess=True)
print(f"ok: {filename}")

if not os.path.exists(f"{directory}/sessions"):
    os.mkdir(f"{directory}/sessions")
if not os.path.exists(f"{directory}/import"):
    os.mkdir(f"{directory}/import")
if not os.path.exists(f"{directory}/export"):
    os.mkdir(f"{directory}/export")
if not os.path.exists(f"{directory}/files"):
    os.mkdir(f"{directory}/files")


def user_panel(message, text=None, reply_to_message_id=None):
    if not text:
        text = "ناحیه کاربری:"
    message.reply_html(
        text=text,
        reply_to_message_id=reply_to_message_id,
        reply_markup={'resize_keyboard': True,'keyboard': [
            [{'text': "📋 سفارش ها"}, {'text': "➕ ایجاد سفارش"}],
            [{'text': "📋 اکانت ها"}, {'text': "➕ افزودن اکانت"}],
            [{'text': "‏📋 API ها"}, {'text': "➕ افزودن API"}],
            [{'text': "📋 دسته بندی ها"}, {'text': "➕ ایجاد دسته بندی"}],
            [{'text': "👤 کاربر"}, {'text': "🔮 آنالیز"}, {'text': "⚙️ تنظیمات"}],
            [{'text': "📩 پیام‌ها"}, {'text': "📣 کانال کش"}]
        ]}
    )


def admin_reply_queue(mbot_id, target_id=None, target_username=None, text="", reply_to_message_id=None, reply_to_inbox_id=None):
    """Enqueue a reply to be sent by a specific mbot.

    mbot_id: mbots.id
    target_id: numeric telegram user id
    target_username: @username
    text: message text
    reply_to_message_id: optional message id to reply to on target side
    """
    try:
        cs = utl.Database().data()
        created_at = int(time.time())
        # Use parameterized query to avoid SQL injection
        sql = f"INSERT INTO {utl.outbox} (mbot_id,target_id,target_username,reply_to_message_id,reply_to_inbox_id,text,status,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        params = (mbot_id, target_id, target_username, reply_to_message_id, reply_to_inbox_id, text, 'new', created_at)
        cs.execute(sql, params)
        # attempt to obtain inserted id for better tracing
        try:
            inserted_id = getattr(cs, 'lastrowid', None)
        except Exception:
            inserted_id = None
        try:
            logger.info("Enqueued outbox id=%s: mbot_id=%s target_id=%s target_username=%s reply_to_inbox=%s len_text=%s", inserted_id, mbot_id, target_id, target_username, reply_to_inbox_id, (len(text) if text else 0))
        except Exception:
            pass
        return inserted_id if inserted_id is not None else True
    except Exception as e:
        try:
            print(f"admin_reply_queue error: {e}")
        except:
            pass
        return False


def callbackquery_process(update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
    bot = context.bot
    query = update.callback_query
    message = query.message
    message_id = message.message_id
    from_id = query.from_user.id
    chat_id = message.chat.id
    data = query.data
    ex_data = data.split(';')
    timestamp = int(time.time())

    if data == "test":
        return
    if data == "nazan":
        return query.answer("Do not touch 😕")
    
    cs = utl.Database()
    cs = cs.data()

    cs.execute(f"SELECT * FROM {utl.admin}")
    row_admin = cs.fetchone()
    cs.execute(f"SELECT * FROM {utl.users} WHERE user_id={from_id}")
    row_user = cs.fetchone()
    
    if from_id in utl.admins or row_user['status'] == 1:
        # handle toggling of inbox block/unblock for a thread
        if ex_data[0] == 'inbox_toggle_block':
            # callback_data: inbox_toggle_block;<mbot_id>;<thread_id>;<typ>;<page>
            try:
                mbot_id = int(ex_data[1])
                thread_id = ex_data[2]
                typ = ex_data[3] if len(ex_data) > 3 else 'new'
                page = int(ex_data[4]) if len(ex_data) > 4 else 1
            except Exception:
                return query.answer(text="❌ دستور نامعتبر", show_alert=True)
            # thread_id is typically the sender's numeric id (string) or a username.
            # Be permissive: accept negative ids, strip leading '@' from usernames.
            try:
                import re
                thread_raw = thread_id
                # normalize username if present
                candidate_username = thread_raw.lstrip('@') if isinstance(thread_raw, str) else None
                # detect integer (allow optional leading minus)
                if isinstance(thread_raw, str) and re.match(r'^-?\d+$', thread_raw):
                    from_id_val = int(thread_raw)
                    username_val = None
                else:
                    # fallback to username field (strip @ to match listener storage)
                    from_id_val = None
                    username_val = candidate_username if candidate_username else None

                # check existing block row
                if from_id_val is not None:
                    cs.execute(f"SELECT COUNT(*) as cnt FROM {utl.inbox_blocked} WHERE mbot_id=%s AND from_id=%s", (mbot_id, from_id_val))
                else:
                    cs.execute(f"SELECT COUNT(*) as cnt FROM {utl.inbox_blocked} WHERE mbot_id=%s AND from_username=%s", (mbot_id, username_val))
                rowb = cs.fetchone()
                exists = int(rowb['cnt']) > 0 if rowb and rowb.get('cnt') is not None else False

                if exists:
                    # delete block entry
                    if from_id_val is not None:
                        cs.execute(f"DELETE FROM {utl.inbox_blocked} WHERE mbot_id=%s AND from_id=%s", (mbot_id, from_id_val))
                    else:
                        cs.execute(f"DELETE FROM {utl.inbox_blocked} WHERE mbot_id=%s AND from_username=%s", (mbot_id, username_val))
                    msg = '✅ شنود برای این فرد غیرفعال شد'
                else:
                    now = int(time.time())
                    if from_id_val is not None:
                        cs.execute(f"INSERT INTO {utl.inbox_blocked} (mbot_id,from_id,created_at) VALUES (%s,%s,%s)", (mbot_id, from_id_val, now))
                    else:
                        cs.execute(f"INSERT INTO {utl.inbox_blocked} (mbot_id,from_username,created_at) VALUES (%s,%s,%s)", (mbot_id, username_val, now))
                    msg = '✅ شنود برای این فرد بلاک شد'
            except Exception as e:
                # surface a helpful error to the admin so debugging is easier
                try:
                    err_text = str(e)
                except:
                    err_text = 'unknown error'
                return query.answer(text=f"❌ خطا هنگام تغییر وضعیت بلاک: {err_text}", show_alert=True)
            # after toggle, re-render the threads list (📂 گفتگوها) so admin returns to that page
            try:
                where_clause = "AND processed=0" if typ == 'new' else ("AND processed=1" if typ == 'read' else "")
                offset2 = (page - 1) * utl.step_page
                cs.execute(f"SELECT COUNT(DISTINCT thread_id) as cnt FROM {utl.inbox} WHERE mbot_id=%s {where_clause}", (mbot_id,))
                total_row2 = cs.fetchone()
                total_threads2 = int(total_row2['cnt']) if (total_row2 and total_row2.get('cnt') is not None) else 0

                sql2 = f"SELECT thread_id, MAX(from_id) as from_id, MAX(from_username) as from_username, MAX(from_first_name) as from_first_name, MAX(from_last_name) as from_last_name, COUNT(*) as cnt, MAX(created_at) as last_created FROM {utl.inbox} WHERE mbot_id=%s {where_clause} GROUP BY thread_id ORDER BY last_created DESC LIMIT %s,%s"
                cs.execute(sql2, (mbot_id, offset2, utl.step_page))
                threads2 = cs.fetchall()
                output2 = f"📂 گفتگوها ({total_threads2})\n\n"
                kb_out = []
                for th2 in threads2:
                    fn = th2.get('from_first_name') or ''
                    ln = th2.get('from_last_name') or ''
                    fullname2 = (f"{fn} {ln}".strip()) if (fn or ln) else ''
                    uname2 = th2.get('from_username') or ''
                    nid2 = str(th2.get('from_id')) if th2.get('from_id') else ''
                    display2 = uname2 if uname2 else (fullname2 if fullname2 else nid2)
                    try:
                        dt2 = jdatetime.datetime.fromtimestamp(th2['last_created']).astimezone(datetime.timezone(datetime.timedelta(hours=3, minutes=30))).strftime('%Y/%m/%d %H:%M')
                    except:
                        dt2 = str(th2['last_created'])
                    output2 += f"👤 {display2} — {th2['cnt']} \n آخرین پیام: {dt2}\n"
                    if fullname2:
                        output2 += f"نام: {fullname2}\n"
                    if uname2:
                        output2 += f"یوزرنیم: @{uname2.lstrip('@')}\n"
                    if nid2:
                        output2 += f"آیدی: {nid2}\n"
                    output2 += "\n"

                    cb_view2 = f'inbox_thread;{typ};{mbot_id};{th2["thread_id"]};1'
                    cb_mark2 = f'markreadthread;{mbot_id};{th2["thread_id"]}'
                    try:
                        if nid2 and nid2.isdigit():
                            cs.execute(f"SELECT COUNT(*) as cnt_block FROM {utl.inbox_blocked} WHERE mbot_id=%s AND from_id=%s", (mbot_id, int(nid2)))
                        else:
                            cs.execute(f"SELECT COUNT(*) as cnt_block FROM {utl.inbox_blocked} WHERE mbot_id=%s AND from_username=%s", (mbot_id, uname2))
                        brow3 = cs.fetchone()
                        is_blocked2 = int(brow3['cnt_block']) > 0 if brow3 and brow3.get('cnt_block') is not None else False
                    except Exception:
                        is_blocked2 = False
                    block_label2 = '🔓 آنبلاک شنود' if is_blocked2 else '🔒 بلاک شنود'
                    block_cb2 = f'inbox_toggle_block;{mbot_id};{th2["thread_id"]};{typ};{page}'

                    kb_out.append([
                        {'text': f"{display2} ({th2['cnt']})", 'callback_data': cb_view2},
                        {'text': '✅ خوانده', 'callback_data': cb_mark2},
                        {'text': block_label2, 'callback_data': block_cb2}
                    ])

                pages_total2 = (total_threads2 + utl.step_page - 1) // utl.step_page
                nav2 = []
                if page > 1:
                    nav2.append({'text': '⬅️ قبلی', 'callback_data': f'inbox_acc;{typ};{mbot_id};{page-1}'})
                if page < pages_total2:
                    nav2.append({'text': 'بعدی ➡️', 'callback_data': f'inbox_acc;{typ};{mbot_id};{page+1}'})
                if nav2:
                    kb_out.append(nav2)
                kb_out.append([{'text': 'بازگشت', 'callback_data': f'inbox_menu;{typ}'}])
                try:
                    message.edit_text(text=output2, parse_mode='HTML', disable_web_page_preview=True, reply_markup={'inline_keyboard': kb_out})
                except Exception:
                    pass
                return query.answer(text=msg, show_alert=False)
            except Exception:
                return query.answer(text=msg, show_alert=False)

        # callback action: mark inbox message as read
        if ex_data[0] == 'markread':
            try:
                inbox_id = int(ex_data[1])
                # optional page parameter passed from inbox views
                page = int(ex_data[2]) if len(ex_data) > 2 else 1
                if page < 1:
                    page = 1
            except:
                return query.answer(text="❌ شناسه نامعتبر", show_alert=True)
            try:
                # fetch the inbox row to know mbot_id and thread_id and previous processed state
                cs.execute(f"SELECT mbot_id,thread_id,processed FROM {utl.inbox} WHERE id=%s", (inbox_id,))
                row = cs.fetchone()
                if not row:
                    return query.answer(text="❌ پیام یافت نشد", show_alert=True)
                mbot_id = row['mbot_id']
                thread_id = row['thread_id']
                prev_processed = int(row['processed']) if row.get('processed') is not None else 0

                # perform update
                cs.execute(f"UPDATE {utl.inbox} SET processed=1 WHERE id=%s", (inbox_id,))

                # decide typ based on previous processed value (assume admin was viewing that category)
                typ = 'new' if prev_processed == 0 else 'read'

                # check remaining messages in the same thread for this typ
                proc_val = 0 if typ == 'new' else 1
                cs.execute(f"SELECT COUNT(*) as cnt FROM {utl.inbox} WHERE mbot_id=%s AND thread_id=%s AND processed=%s", (mbot_id, thread_id, proc_val))
                rem_row = cs.fetchone()
                rem_count = int(rem_row['cnt']) if (rem_row and rem_row.get('cnt') is not None) else 0

                if rem_count > 0:
                    # rebuild the thread view (same as inbox_thread handler) with pagination
                    offset = (page - 1) * utl.step_page
                    # total messages for pagination
                    cs.execute(f"SELECT COUNT(*) as cnt FROM {utl.inbox} WHERE mbot_id=%s AND thread_id=%s AND processed=%s", (mbot_id, thread_id, proc_val))
                    total_row = cs.fetchone()
                    total_msgs = int(total_row['cnt']) if (total_row and total_row.get('cnt') is not None) else 0

                    sql_msgs = f"SELECT id,from_id,from_username,text,created_at,processed FROM {utl.inbox} WHERE mbot_id=%s AND thread_id=%s AND processed=%s ORDER BY created_at DESC LIMIT %s,%s"
                    cs.execute(sql_msgs, (mbot_id, thread_id, proc_val, offset, utl.step_page))
                    msgs = cs.fetchall()
                    output = "📨 پیام های گفتگو:\n\n"
                    kb = []
                    for m in msgs:
                        display = m['from_username'] if m['from_username'] else str(m['from_id'])
                        text_snip = (m['text'][:60] + '...') if m['text'] and len(m['text'])>60 else (m['text'] if m['text'] else '')
                        try:
                            dt = jdatetime.datetime.fromtimestamp(m['created_at']).astimezone(datetime.timezone(datetime.timedelta(hours=3, minutes=30))).strftime('%Y/%m/%d %H:%M')
                        except:
                            dt = str(m['created_at'])
                        output += f"{display}: {text_snip} — {dt}\n"
                        cb_more = f"inbox_more;{m['id']};{mbot_id};{typ};{thread_id};{page}"
                        cb_reply = f"inbox_select;{m['id']};{mbot_id}"
                        cb_mark = f"markread;{m['id']};{page}"
                        # prepare snippet for label
                        raw_text = m.get('text') or ''
                        try:
                            words = raw_text.strip().split()
                            if len(words) >= 2:
                                snippet = ' '.join(words[:2])
                            else:
                                snippet = raw_text.strip()[:40]
                            snippet = snippet.replace('\n',' ').replace('\r','')
                        except Exception:
                            snippet = '(بدون متن)'
                        if not snippet:
                            snippet = '(بدون متن)'
                        reply_label = f"پاسخ به {snippet}"
                        kb.append([
                            {'text': 'بیشتر', 'callback_data': cb_more},
                            {'text': reply_label, 'callback_data': cb_reply},
                            {'text': '✅ خوانده', 'callback_data': cb_mark}
                        ])
                    kb.append([{'text': 'علامت خوانده شده (گفتگو) ✅', 'callback_data': f"markreadthread;{mbot_id};{thread_id}"}])
                    # pagination for messages (preserve prev/next after marking)
                    pages_total = (total_msgs + utl.step_page - 1) // utl.step_page
                    nav = []
                    if page > 1:
                        nav.append({'text': '⬅️ قبلی', 'callback_data': f'inbox_thread;{typ};{mbot_id};{thread_id};{page-1}'})
                    if page < pages_total:
                        nav.append({'text': 'بعدی ➡️', 'callback_data': f'inbox_thread;{typ};{mbot_id};{thread_id};{page+1}'})
                    if nav:
                        kb.append(nav)
                    kb.append([{'text': 'بازگشت', 'callback_data': f'inbox_acc;{typ};{mbot_id};{page}'}])
                    try:
                        message.edit_text(text=output, parse_mode='HTML', disable_web_page_preview=True, reply_markup={'inline_keyboard': kb})
                    except Exception:
                        pass
                    return query.answer(text="✅ پیام علامت خوانده شده شد", show_alert=False)
                else:
                    # no messages left in this thread for that typ -> check if other threads exist for this mbot
                    sql = f"SELECT thread_id, COUNT(*) as cnt FROM {utl.inbox} WHERE mbot_id=%s AND processed=%s GROUP BY thread_id ORDER BY MAX(created_at) DESC LIMIT %s"
                    cs.execute(sql, (mbot_id, proc_val, utl.step_page))
                    threads = cs.fetchall()
                    if threads:
                        # show account's threads list
                        output = f"📂 گفتگوها ({len(threads)})\n\n"
                        kb = []
                        for th in threads:
                            # attempt to get a display name
                            cs.execute(f"SELECT MAX(from_username) as from_username, MAX(from_id) as from_id, MAX(created_at) as last_created FROM {utl.inbox} WHERE mbot_id=%s AND thread_id=%s", (mbot_id, th['thread_id']))
                            info = cs.fetchone()
                            display = info['from_username'] if info and info.get('from_username') else str(info.get('from_id') if info else th['thread_id'])
                            cb_view = 'inbox_thread;' + typ + ';' + str(mbot_id) + ';' + str(th['thread_id']) + ';1'
                            cb_mark = 'markreadthread;' + str(mbot_id) + ';' + str(th['thread_id'])
                            output += f"👤 {display} — {th['cnt']} پیام\n"
                            kb.append([
                                {'text': f"{display} ({th['cnt']})", 'callback_data': cb_view},
                                {'text': '✅ خوانده', 'callback_data': cb_mark}
                            ])
                        kb.append([{'text': 'بازگشت', 'callback_data': 'inbox_menu;'+typ}])
                        try:
                            message.edit_text(text=output, parse_mode='HTML', disable_web_page_preview=True, reply_markup={'inline_keyboard': kb})
                        except Exception:
                            pass
                        return query.answer(text="✅ پیام علامت خوانده شده شد", show_alert=False)
                    else:
                        # nothing left for this typ on this account -> return to inbox menu
                        try:
                            message.edit_text(text=f"📩 پیام‌ها — {('جدیدها' if typ=='new' else 'خوانده شده‌ها')}\n\nهیچ پیامی یافت نشد.", parse_mode='HTML', reply_markup={'inline_keyboard': [[{'text': 'بازگشت', 'callback_data': 'inbox_menu;'+typ}]]})
                        except Exception:
                            pass
                        return query.answer(text="✅ پیام علامت خوانده شده شد", show_alert=False)
            except Exception:
                return query.answer(text="❌ خطا هنگام علامت‌گذاری", show_alert=True)
        if ex_data[0] == 'markreadthread':
            # callback_data: markreadthread;<mbot_id>;<thread_id>
            try:
                mbot_id = int(ex_data[1])
                thread_id = ex_data[2]
            except Exception:
                return query.answer(text="❌ دستور نامعتبر", show_alert=True)
            try:
                # determine whether this thread had new messages (processed=0) before update
                cs.execute(f"SELECT COUNT(*) as cnt_new FROM {utl.inbox} WHERE mbot_id=%s AND thread_id=%s AND processed=0", (mbot_id, thread_id))
                cnt_row = cs.fetchone()
                cnt_new = int(cnt_row['cnt_new']) if (cnt_row and cnt_row.get('cnt_new') is not None) else 0
                typ = 'new' if cnt_new > 0 else 'read'

                # mark all in thread as read
                cs.execute(f"UPDATE {utl.inbox} SET processed=1 WHERE mbot_id=%s AND thread_id=%s", (mbot_id, thread_id))

                # check if there are remaining threads for this mbot with that typ
                proc_val = 0 if typ == 'new' else 1
                sql = f"SELECT thread_id, COUNT(*) as cnt FROM {utl.inbox} WHERE mbot_id=%s AND processed=%s GROUP BY thread_id ORDER BY MAX(created_at) DESC LIMIT %s"
                cs.execute(sql, (mbot_id, proc_val, utl.step_page))
                threads = cs.fetchall()
                if threads:
                    output = f"📂 گفتگوها ({len(threads)})\n\n"
                    kb = []
                    for th in threads:
                        cs.execute(f"SELECT MAX(from_username) as from_username, MAX(from_id) as from_id FROM {utl.inbox} WHERE mbot_id=%s AND thread_id=%s", (mbot_id, th['thread_id']))
                        info = cs.fetchone()
                        display = info['from_username'] if info and info.get('from_username') else str(info.get('from_id') if info else th['thread_id'])
                        cb_view = 'inbox_thread;' + typ + ';' + str(mbot_id) + ';' + str(th['thread_id']) + ';1'
                        cb_mark = 'markreadthread;' + str(mbot_id) + ';' + str(th['thread_id'])
                        output += f"👤 {display} — {th['cnt']} پیام\n"
                        kb.append([
                            {'text': f"{display} ({th['cnt']})", 'callback_data': cb_view},
                            {'text': '✅ خوانده', 'callback_data': cb_mark}
                        ])
                    kb.append([{'text': 'بازگشت', 'callback_data': 'inbox_menu;'+typ}])
                    try:
                        message.edit_text(text=output, parse_mode='HTML', disable_web_page_preview=True, reply_markup={'inline_keyboard': kb})
                    except Exception:
                        pass
                    return query.answer(text="✅ همه پیام‌های گفتگو علامت خوانده شده شد", show_alert=False)
                else:
                    # no threads left for this typ -> go back to inbox menu
                    try:
                        message.edit_text(text=f"📩 پیام‌ها — {('جدیدها' if typ=='new' else 'خوانده شده‌ها')}\n\nهیچ پیامی یافت نشد.", parse_mode='HTML', reply_markup={'inline_keyboard': [[{'text': 'بازگشت', 'callback_data': 'inbox_menu;'+typ}]]})
                    except Exception:
                        pass
                    return query.answer(text="✅ همه پیام‌های گفتگو علامت خوانده شده شد", show_alert=False)
            except Exception:
                return query.answer(text="❌ خطا هنگام علامت‌گذاری گفتگو", show_alert=True)

        # Inbox UI flow: inbox_menu;TYPE -> list accounts
        # TYPE is 'new' | 'read' | 'all'
        if ex_data[0] == 'inbox_menu':
            try:
                typ = ex_data[1]
            except:
                typ = 'new'
            # Show only accounts that have messages for the requested type (new/read/all)
            where_clause = "AND i.processed=0" if typ == 'new' else ("AND i.processed=1" if typ == 'read' else "")
            sql = f"SELECT m.id,m.phone,(SELECT COUNT(*) FROM {utl.inbox} i WHERE i.mbot_id=m.id {where_clause}) as cnt FROM {utl.mbots} m WHERE m.user_id IS NOT NULL ORDER BY m.id DESC"
            cs.execute(sql)
            acc_rows = cs.fetchall()
            # filter out accounts with zero count
            accounts = [a for a in acc_rows if a.get('cnt') and int(a.get('cnt')) > 0]
            if not accounts:
                return query.answer(text="⛔️ پیامی یافت نشد", show_alert=True)
            kb = []
            for acc in accounts:
                label = f"{acc['phone']} ({acc['cnt']})"
                cb = 'inbox_acc;' + typ + ';' + str(acc['id'])
                # two-column style not needed here; single button per account that shows count
                kb.append([{'text': label, 'callback_data': cb}])
            kb.append([{'text': 'بازگشت', 'callback_data': 'menu'}])
            return message.edit_text(text=f"📩 پیام‌ها — {('جدیدها' if typ=='new' else ('خوانده شده‌ها' if typ=='read' else 'همه'))}\n\nلیست اکانت ها:", parse_mode='HTML', reply_markup={'inline_keyboard': kb})

        # inbox_acc;TYPE;MBOT_ID -> list threads (grouped by thread_id)
        if ex_data[0] == 'inbox_acc':
            # inbox_acc;TYPE;MBOT_ID[;PAGE]
            try:
                typ = ex_data[1]
                mbot_id = int(ex_data[2])
            except:
                return query.answer(text="❌ دستور نامعتبر", show_alert=True)
            # page handling
            try:
                page = int(ex_data[3]) if len(ex_data) > 3 else 1
                if page < 1:
                    page = 1
            except:
                page = 1

            offset = (page - 1) * utl.step_page
            # determine processed filter
            where_clause = "AND processed=0" if typ == 'new' else ("AND processed=1" if typ == 'read' else "")
            # total threads count for this mbot and typ (for pagination)
            cs.execute(f"SELECT COUNT(DISTINCT thread_id) as cnt FROM {utl.inbox} WHERE mbot_id=%s {where_clause}", (mbot_id,))
            total_row = cs.fetchone()
            total_threads = int(total_row['cnt']) if (total_row and total_row.get('cnt') is not None) else 0

            # aggregate non-grouped columns using MAX to satisfy ONLY_FULL_GROUP_BY
            sql = f"SELECT thread_id, MAX(from_id) as from_id, MAX(from_username) as from_username, MAX(from_first_name) as from_first_name, MAX(from_last_name) as from_last_name, COUNT(*) as cnt, MAX(created_at) as last_created FROM {utl.inbox} WHERE mbot_id=%s {where_clause} GROUP BY thread_id ORDER BY last_created DESC LIMIT %s,%s"
            params = (mbot_id, offset, utl.step_page)
            cs.execute(sql, params)
            threads = cs.fetchall()
            if not threads:
                return query.answer(text="⛔️ پیامی یافت نشد", show_alert=True)
            kb = []
            output = f"📂 گفتگوها ({total_threads})\n\n"
            for th in threads:
                # prepare display and richer metadata lines
                first_name = th.get('from_first_name') if th.get('from_first_name') is not None else ''
                last_name = th.get('from_last_name') if th.get('from_last_name') is not None else ''
                fullname = (f"{first_name} {last_name}".strip()) if (first_name or last_name) else ''
                username = th.get('from_username') if th.get('from_username') else ''
                numeric_id = str(th.get('from_id')) if th.get('from_id') else ''
                display = username if username else (fullname if fullname else numeric_id)
                # format time
                try:
                    dt = jdatetime.datetime.fromtimestamp(th['last_created']).astimezone(datetime.timezone(datetime.timedelta(hours=3, minutes=30))).strftime('%Y/%m/%d %H:%M')
                except:
                    dt = str(th['last_created'])
                # build a neat multi-line block for the thread summary
                output += f"👤 {display} — {th['cnt']}\n آخرین پیام: {dt}\n"
                if fullname:
                    output += f"نام: {fullname}\n"
                if username:
                    output += f"یوزرنیم: @{username.lstrip('@')}\n"
                if numeric_id:
                    output += f"آیدی: {numeric_id}\n"
                output += "\n"
                # include page param when opening thread (start at page 1)
                cb_view = f'inbox_thread;{typ};{mbot_id};{th["thread_id"]};1'
                cb_mark = f'markreadthread;{mbot_id};{th["thread_id"]}'
                # determine whether this thread's sender is currently blocked for this mbot
                try:
                    # try numeric id first
                    if numeric_id and numeric_id.isdigit():
                        cs.execute(f"SELECT COUNT(*) as cnt_block FROM {utl.inbox_blocked} WHERE mbot_id=%s AND from_id=%s", (mbot_id, int(numeric_id)))
                    else:
                        cs.execute(f"SELECT COUNT(*) as cnt_block FROM {utl.inbox_blocked} WHERE mbot_id=%s AND from_username=%s", (mbot_id, username))
                    brow = cs.fetchone()
                    is_blocked = int(brow['cnt_block']) > 0 if brow and brow.get('cnt_block') is not None else False
                except Exception:
                    is_blocked = False

                block_label = '🔓 آنبلاک شنود' if is_blocked else '🔒 بلاک شنود'
                block_cb = f'inbox_toggle_block;{mbot_id};{th["thread_id"]};{typ};{page}'
                # show three buttons per row: view messages, mark-as-read, block/unblock
                kb.append([
                    {'text': f"{display} ({th['cnt']})", 'callback_data': cb_view},
                    {'text': '✅ خوانده', 'callback_data': cb_mark},
                    {'text': block_label, 'callback_data': block_cb}
                ])

            # pagination prev/next
            pages_total = (total_threads + utl.step_page - 1) // utl.step_page
            nav = []
            if page > 1:
                nav.append({'text': '⬅️ قبلی', 'callback_data': f'inbox_acc;{typ};{mbot_id};{page-1}'})
            if page < pages_total:
                nav.append({'text': 'بعدی ➡️', 'callback_data': f'inbox_acc;{typ};{mbot_id};{page+1}'})
            if nav:
                kb.append(nav)

            kb.append([{'text': 'بازگشت', 'callback_data': f'inbox_menu;{typ}'}])
            return message.edit_text(text=output, parse_mode='HTML', disable_web_page_preview=True, reply_markup={'inline_keyboard': kb})

        # inbox_thread;TYPE;MBOT_ID;THREAD_ID -> list messages in the thread
        if ex_data[0] == 'inbox_thread':
            # inbox_thread;TYPE;MBOT_ID;THREAD_ID[;PAGE]
            try:
                typ = ex_data[1]
                mbot_id = int(ex_data[2])
                thread_id = ex_data[3]
            except:
                return query.answer(text="❌ دستور نامعتبر", show_alert=True)
            try:
                page = int(ex_data[4]) if len(ex_data) > 4 else 1
                if page < 1:
                    page = 1
            except:
                page = 1

            # respect the requested type (new/read/all)
            where_proc = "AND processed=0" if typ == 'new' else ("AND processed=1" if typ == 'read' else "")
            offset = (page - 1) * utl.step_page

            # total messages for pagination
            cs.execute(f"SELECT COUNT(*) as cnt FROM {utl.inbox} WHERE mbot_id=%s AND thread_id=%s {where_proc}", (mbot_id, thread_id))
            total_row = cs.fetchone()
            total_msgs = int(total_row['cnt']) if (total_row and total_row.get('cnt') is not None) else 0

            sql_msgs = f"SELECT id,from_id,from_username,text,created_at,processed FROM {utl.inbox} WHERE mbot_id=%s AND thread_id=%s {where_proc} ORDER BY created_at DESC LIMIT %s,%s"
            cs.execute(sql_msgs, (mbot_id, thread_id, offset, utl.step_page))
            msgs = cs.fetchall()
            if not msgs:
                return query.answer(text="⛔️ پیامی یافت نشد", show_alert=True)
            kb = []

            output = "📨 پیام های گفتگو:\n\n"

            for m in msgs:
                text_snip = (
                    (m['text'][:60] + '...') 
                    if m['text'] and len(m['text']) > 60 
                    else (m['text'] or '')
                )

                try:
                    dt = jdatetime.datetime.fromtimestamp(
                        m['created_at']
                    ).astimezone(
                        datetime.timezone(datetime.timedelta(hours=3, minutes=30))
                    ).strftime('%Y/%m/%d %H:%M')
                except:
                    dt = str(m['created_at'])

                output += f"{dt}:\n{text_snip}\n\n"

                cb_more = f"inbox_more;{m['id']};{mbot_id};{typ};{thread_id};{page}"
                cb_reply = f"inbox_select;{m['id']};{mbot_id}"
                cb_mark = f"markread;{m['id']};{page}"

                # prepare a short snippet (first two words or first 40 chars) for the reply button label
                raw_text = m.get('text') or ''
                snippet = ''
                try:
                    words = raw_text.strip().split()
                    if len(words) >= 2:
                        snippet = ' '.join(words[:2])
                    else:
                        snippet = raw_text.strip()[:40]
                    snippet = snippet.replace('\n', ' ').replace('\r', '')
                except Exception:
                    snippet = ''
                if not snippet:
                    snippet = '(بدون متن)'
                reply_label = f"پاسخ به {snippet}"

                kb.append([
                    {'text': 'بیشتر', 'callback_data': cb_more},
                    {'text': reply_label, 'callback_data': cb_reply},
                    {'text': '✅ خوانده', 'callback_data': cb_mark}
                ])

            # mark thread read button
            kb.append([{'text': 'علامت خوانده شده (گفتگو) ✅', 'callback_data': f"markreadthread;{mbot_id};{thread_id}"}])

            # pagination for messages
            pages_total = (total_msgs + utl.step_page - 1) // utl.step_page
            nav = []
            if page > 1:
                nav.append({'text': '⬅️ قبلی', 'callback_data': f'inbox_thread;{typ};{mbot_id};{thread_id};{page-1}'})
            if page < pages_total:
                nav.append({'text': 'بعدی ➡️', 'callback_data': f'inbox_thread;{typ};{mbot_id};{thread_id};{page+1}'})
            if nav:
                kb.append(nav)

            kb.append([{'text': 'بازگشت', 'callback_data': f'inbox_acc;{typ};{mbot_id};{page}'}])
            return message.edit_text(text=output, parse_mode='HTML', disable_web_page_preview=True, reply_markup={'inline_keyboard': kb})

        # inbox_more;INBOX_ID;MBOT_ID;TYPE;THREAD_ID -> show full message and actions
        if ex_data[0] == 'inbox_more':
            try:
                inbox_id = int(ex_data[1])
                mbot_id = int(ex_data[2])
                typ = ex_data[3]
                thread_id = ex_data[4]
                page = int(ex_data[5]) if len(ex_data) > 5 else 1
                if page < 1:
                    page = 1
            except Exception:
                return query.answer(text="❌ دستور نامعتبر", show_alert=True)
            cs.execute(f"SELECT id,from_id,from_username,text,created_at,processed FROM {utl.inbox} WHERE id=%s", (inbox_id,))
            row_msg = cs.fetchone()
            if not row_msg:
                return query.answer(text="⛔️ پیام یافت نشد", show_alert=True)
            sender = row_msg['from_username'] if row_msg['from_username'] else str(row_msg['from_id'])
            try:
                dt = jdatetime.datetime.fromtimestamp(row_msg['created_at']).astimezone(datetime.timezone(datetime.timedelta(hours=3, minutes=30))).strftime('%Y/%m/%d %H:%M')
            except:
                dt = str(row_msg['created_at'])
            text_full = row_msg['text'] if row_msg['text'] else '(بدون متن)'
            out = f"📩 پیام کامل از {sender} — {dt}\n\n{text_full}"
            # actions: reply, mark read, back
            kb = [
                [
                    {'text': 'پاسخ', 'callback_data': f'inbox_select;{inbox_id};{mbot_id}'},
                    {'text': '✅ خوانده', 'callback_data': f'markread;{inbox_id};{page}'}
                ],
                [{'text': 'بازگشت', 'callback_data': f'inbox_thread;{typ};{mbot_id};{thread_id};{page}'}]
            ]
            return message.edit_text(text=out, parse_mode='HTML', disable_web_page_preview=True, reply_markup={'inline_keyboard': kb})

        # inbox_select;INBOX_ID;MBOT_ID -> set admin to reply state
        if ex_data[0] == 'inbox_select':
            try:
                inbox_id = int(ex_data[1])
                mbot_id = int(ex_data[2])
            except:
                return query.answer(text="❌ دستور نامعتبر", show_alert=True)
            # set user's step so that next message will be enqueued as reply
            step_val = f"reply_inbox;{inbox_id};{mbot_id}"
            try:
                cs.execute(f"UPDATE {utl.users} SET step=%s WHERE user_id=%s", (step_val, from_id))
            except:
                pass
            try:
                # fetch the inbox message to show a small quoted context so admin knows what they're replying to
                cs.execute(f"SELECT text,from_username,from_id FROM {utl.inbox} WHERE id=%s", (inbox_id,))
                row_preview = cs.fetchone()
                snippet = ''
                if row_preview and row_preview.get('text'):
                    raw = str(row_preview.get('text'))
                    # take first two words if possible, otherwise first 40 chars
                    words = raw.strip().split()
                    if len(words) >= 2:
                        snippet = ' '.join(words[:2])
                    else:
                        snippet = raw.strip()[:40]
                    # sanitize newlines
                    snippet = snippet.replace('\n', ' ').replace('\r', '')
                else:
                    snippet = '(بدون متن)'
                sender_label = None
                if row_preview:
                    sender_label = row_preview.get('from_username') if row_preview.get('from_username') else str(row_preview.get('from_id') or '')
                sender_label = sender_label if sender_label else ''
                prompt_text = f"در حال پاسخ به: <b>{snippet}</b>\n\nلطفا پیام پاسخ را ارسال کنید:"
                # send prompt with context (HTML formatting)
                bot.send_message(chat_id=from_id, text=prompt_text, parse_mode='HTML', reply_markup={'resize_keyboard': True, 'keyboard': [[{'text': utl.menu_var}]]})
            except Exception:
                # fallback simpler prompt
                try:
                    bot.send_message(chat_id=from_id, text="لطفا پیام پاسخ را ارسال کنید:", reply_markup={'resize_keyboard': True, 'keyboard': [[{'text': utl.menu_var}]]})
                except:
                    pass
            return query.answer(text="✅ آماده برای ارسال پاسخ — اکنون پیام خود را ارسال کنید", show_alert=False)
        if ex_data[0] == 'pg':
            if ex_data[1] == 'accounts':
                selected_pages = (int(ex_data[2]) - 1) * utl.step_page
                i = selected_pages + 1
                cs.execute(f"SELECT * FROM {utl.mbots} WHERE user_id IS NOT NULL ORDER BY id DESC LIMIT {selected_pages},{utl.step_page}")
                result = cs.fetchall()
                if not result:
                    return query.answer(text="⛔️ صفحه دیگری وجود ندارد", show_alert=True)
                
                cs.execute(f"SELECT COUNT(*) as count FROM {utl.mbots} WHERE user_id IS NOT NULL")
                rowcount = cs.fetchone()['count']
                output = f"📜 لیست همه اکانت ها ({rowcount:,})\n\n"
                for row in result:
                    cs.execute(f"SELECT * FROM {utl.cats} WHERE id={row['cat_id']}")
                    row_cats = cs.fetchone()
                    if row['status'] == 2:
                        output += f"{i}. شماره: <code>{row['phone']}</code>\n"
                        output += f"⛔ محدودیت: ({utl.convert_time((row['end_restrict'] - timestamp),2)})\n"
                    else:
                        output += f"{i}. شماره: <code>{row['phone']}</code> ({utl.status_mbots[row['status']]})\n"
                    output += f"📂 دسته بندی: ‏/category_{row['id']} ‏({row_cats['name']})\n"
                    output += f"🔸️ وضعیت: /status_{row['id']}\n"
                    output += f"❌ حذف: /delete_{row['id']}\n\n"
                    i += 1
                ob = utl.Pagination(update, "accounts", output, utl.step_page, rowcount)
                return ob.process()
            if ex_data[1] == '0':
                selected_pages = (int(ex_data[2]) - 1) * utl.step_page
                i = selected_pages + 1
                cs.execute(f"SELECT * FROM {utl.mbots} WHERE status=0 AND user_id IS NOT NULL ORDER BY last_order_at DESC LIMIT {selected_pages},{utl.step_page}")
                result = cs.fetchall()
                if not result:
                    return query.answer(text="⛔️ صفحه دیگری وجود ندارد", show_alert=True)
                
                cs.execute(f"SELECT COUNT(*) as count FROM {utl.mbots} WHERE status=0 AND user_id IS NOT NULL")
                rowcount = cs.fetchone()['count']
                output = f"📜 لیست اکانت های لاگ اوت شده ({rowcount:,})\n\n"
                for row in result:
                    cs.execute(f"SELECT * FROM {utl.cats} WHERE id={row['cat_id']}")
                    row_cats = cs.fetchone()
                    output += f"{i}. شماره: <code>{row['phone']}</code> ({utl.status_mbots[row['status']]})\n"
                    output += f"📂 دسته بندی: ‏/category_{row['id']} ‏({row_cats['name']})\n"
                    output += f"🔸️ وضعیت: /status_{row['id']}\n"
                    output += f"❌ حذف: /delete_{row['id']}\n\n"
                    i += 1
                ob = utl.Pagination(update, "0", output, utl.step_page, rowcount)
                return ob.process()
            if ex_data[1] == '1':
                selected_pages = (int(ex_data[2]) - 1) * utl.step_page
                i = selected_pages + 1
                cs.execute(f"SELECT * FROM {utl.mbots} WHERE status=1 ORDER BY last_order_at ASC LIMIT {selected_pages},{utl.step_page}")
                result = cs.fetchall()
                if not result:
                    return query.answer(text="⛔️ صفحه دیگری وجود ندارد", show_alert=True)
                
                cs.execute(f"SELECT COUNT(*) as count FROM {utl.mbots} WHERE status=1 AND user_id IS NOT NULL")
                rowcount = cs.fetchone()['count']
                output = f"📜 لیست اکانت های فعال ({rowcount:,})\n\n"
                for row in result:
                    cs.execute(f"SELECT * FROM {utl.cats} WHERE id={row['cat_id']}")
                    row_cats = cs.fetchone()
                    output += f"{i}. شماره: <code>{row['phone']}</code> ({utl.status_mbots[row['status']]})\n"
                    output += f"📂 دسته بندی: ‏/category_{row['id']} ‏({row_cats['name']})\n"
                    output += f"🔸️ وضعیت: /status_{row['id']}\n"
                    output += f"❌ حذف: /delete_{row['id']}\n\n"
                    i += 1
                ob = utl.Pagination(update, "1", output, utl.step_page, rowcount)
                return ob.process()
            if ex_data[1] == '2':
                selected_pages = (int(ex_data[2]) - 1) * utl.step_page
                i = selected_pages + 1
                cs.execute(f"SELECT * FROM {utl.mbots} WHERE status=2 ORDER BY end_restrict ASC LIMIT {selected_pages},{utl.step_page}")
                result = cs.fetchall()
                if not result:
                    return query.answer(text="⛔️ صفحه دیگری وجود ندارد", show_alert=True)
                
                cs.execute(f"SELECT COUNT(*) as count FROM {utl.mbots} WHERE status=2 AND user_id IS NOT NULL")
                rowcount = cs.fetchone()['count']
                output = f"📜 لیست اکانت های محدود شده ({rowcount:,})\n\n"
                for row in result:
                    cs.execute(f"SELECT * FROM {utl.cats} WHERE id={row['cat_id']}")
                    row_cats = cs.fetchone()
                    output += f"{i}. شماره: <code>{row['phone']}</code>\n"
                    output += f"⛔ محدودیت: ({utl.convert_time((row['end_restrict'] - timestamp),2)})\n"
                    output += f"📂 دسته بندی: ‏/category_{row['id']} ‏({row_cats['name']})\n"
                    output += f"🔸️ وضعیت: /status_{row['id']}\n"
                    output += f"❌ حذف: /delete_{row['id']}\n\n"
                    i += 1
                ob = utl.Pagination(update, "2", output, utl.step_page, rowcount)
                return ob.process()
            if ex_data[1] == 'orders':
                selected_pages = (int(ex_data[2]) - 1) * utl.step_page
                i = selected_pages + 1
                cs.execute(f"SELECT * FROM {utl.orders} WHERE status>0 ORDER BY id DESC LIMIT {selected_pages},{utl.step_page}")
                result = cs.fetchall()
                if not result:
                    return query.answer(text="⛔️ صفحه دیگری وجود ندارد", show_alert=True)
                
                now = jdatetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=3, minutes=30)))
                time_today = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
                time_yesterday = time_today - 86400
                cs.execute(f"SELECT COUNT(*) as count FROM {utl.orders}")
                count = cs.fetchone()['count']
                cs.execute(f"SELECT COUNT(*) as count FROM {utl.orders} WHERE created_at>={time_today}")
                orders_count_today = cs.fetchone()['count']
                cs.execute(f"SELECT COUNT(*) as count FROM {utl.orders} WHERE created_at<{time_today} AND created_at>={time_yesterday}")
                orders_count_yesterday = cs.fetchone()['count']

                cs.execute(f"SELECT sum(count_done) FROM {utl.orders} WHERE status=2")
                orders_count_moved_all = cs.fetchone()['sum(count_done)']
                orders_count_moved_all = orders_count_moved_all if orders_count_moved_all is not None else 0
                cs.execute(f"SELECT sum(count_done) FROM {utl.orders} WHERE status=2 AND created_at>={time_today}")
                orders_count_moved_today = cs.fetchone()['sum(count_done)']
                orders_count_moved_today = orders_count_moved_today if orders_count_moved_today is not None else 0
                cs.execute(f"SELECT sum(count_done) FROM {utl.orders} WHERE status=2 AND created_at<{time_today} AND created_at>={time_yesterday}")
                orders_count_moved_yesterday = cs.fetchone()['sum(count_done)']
                orders_count_moved_yesterday = orders_count_moved_yesterday if orders_count_moved_yesterday is not None else 0

                output = f"📋 کل سفارش ها: {count} ({orders_count_moved_all})\n"
                output += f"🟢 سفارش های امروز: {orders_count_today} ({orders_count_moved_today})\n"
                output += f"⚪️ سفارش های دیروز: {orders_count_yesterday} ({orders_count_moved_yesterday})\n\n"
                for row in result:
                    group_link = f"<a href='{row['group_link']}'>{row['group_link'].replace('https://t.me/', '')}</a>" if row['group_link'] is not None else "با فایل انجام شده"
                    output += f"{i}. جزییات: /order_{row['id']}\n"
                    output += f"🔹️ گروه: {group_link}\n"
                    output += f"🔹️ انجام شده / درخواستی: [{row['count_done']} / {row['count']}]\n"
                    output += f"🔹️ وضعیت: {utl.status_orders[row['status']]}\n"
                    output += f"📅️ ایجاد: {jdatetime.datetime.fromtimestamp(row['created_at']).astimezone(datetime.timezone(datetime.timedelta(hours=3, minutes=30))).strftime('%Y/%m/%d %H:%M')}\n\n"
                    i += 1
                ob = utl.Pagination(update, "orders", output, utl.step_page, count)
                return ob.process()
            if ex_data[1] == 'categories':
                selected_pages = (int(ex_data[2]) - 1) * utl.step_page
                i = selected_pages + 1
                cs.execute(f"SELECT * FROM {utl.cats} ORDER BY id DESC LIMIT {selected_pages},{utl.step_page}")
                result = cs.fetchall()
                if not result:
                    return query.answer(text="⛔️ صفحه دیگری وجود ندارد", show_alert=True)
                
                cs.execute(f"SELECT COUNT(*) as count FROM {utl.cats}")
                rowcount = cs.fetchone()['count']
                output = f"📋 دسته بندی ها ({rowcount})\n\n"
                for row in result:
                    cs.execute(f"SELECT COUNT(*) as count FROM {utl.mbots} WHERE cat_id={row['id']}")
                    count_mbots = cs.fetchone()['count']
                    output += f"{i}. ‏{row['name']} ‏({count_mbots} اکانت)\n"
                    output += f"❌ حذف: /DeleteCat_{row['id']}\n\n"
                    i += 1
                ob = utl.Pagination(update, "categories", output, utl.step_page, rowcount)
                return ob.process()
            if ex_data[1] == 'apis':
                selected_pages = (int(ex_data[2]) - 1) * utl.step_page
                i = selected_pages + 1
                cs.execute(f"SELECT * FROM {utl.apis} ORDER BY id DESC LIMIT {selected_pages},{utl.step_page}")
                result = cs.fetchall()
                if not result:
                    return query.answer(text="⛔️ صفحه دیگری وجود ندارد", show_alert=True)
                
                cs.execute(f"SELECT COUNT(*) as count FROM {utl.apis}")
                rowcount = cs.fetchone()['count']
                output = f"‏📜 API ها ({rowcount})\n\n"
                for row in result:
                    output += f"‏🔴️ Api ID: ‏<code>{row['api_id']}</code>\n"
                    output += f"‏🔴️ Api Hash: ‏<code>{row['api_hash']}</code>\n"
                    output += f"❌ حذف: /DeleteApi_{row['id']}\n\n"
                ob = utl.Pagination(update, "apis", output, utl.step_page, rowcount)
                return ob.process()
        if ex_data[0] == "d":
            cs.execute(f"SELECT * FROM {utl.users} WHERE user_id={int(ex_data[1])}")
            row_user_select = cs.fetchone()
            if row_user_select is None:
                query.answer(text="❌ کاربر یافت نشد", show_alert=True)
                return message.delete()
            
            if ex_data[2] == "1" or ((ex_data[2] == "0" or ex_data[2] == "2") and row_user_select['status'] == 1):
                if from_id in utl.admins:
                    cs.execute(f"UPDATE {utl.users} SET status='{ex_data[2]}' WHERE user_id={row_user_select['user_id']}")
                else:
                    return query.answer(text="⛔️ این عملیات مخصوص ادمین اصلی است", show_alert=True)
            elif ex_data[2] == "2" or ex_data[2] == "0":
                cs.execute(f"UPDATE {utl.users} SET status='{ex_data[2]}' WHERE user_id={row_user_select['user_id']}")
            elif ex_data[2] == "sendmsg":
                cs.execute(f"UPDATE {utl.users} SET step='sendmsg;{row_user_select['user_id']}' WHERE user_id={from_id}")
                return message.reply_html(
                    text="پیام را ارسال کنید:",
                    reply_to_message_id=message_id,
                    reply_markup={'resize_keyboard': True,'keyboard': [[{'text': utl.menu_var}]]}
                )
            else:
                return
            
            cs.execute(f"SELECT * FROM {utl.users} WHERE user_id={row_user_select['user_id']}")
            row_user_select = cs.fetchone()
            admin_status = 0 if row_user_select['status'] == 1 else 1
            return message.edit_text(
                text=f"کاربر <a href='tg://user?id={row_user_select['user_id']}'>{row_user_select['user_id']}</a>",
                parse_mode='HTML',
                reply_markup={'inline_keyboard': [
                    [{'text': "ارسال پیام",'callback_data': f"d;{row_user_select['user_id']};sendmsg"}],
                    [{'text': ('ادمین ✅' if row_user_select['status'] == 1 else 'ادمین ❌'), 'callback_data': f"d;{row_user_select['user_id']};{admin_status}"}]
                ]}
            )
        if ex_data[0] == 'settings':
            if ex_data[1] == 'account_password':
                cs.execute(f"UPDATE {utl.users} SET step='{ex_data[0]};{ex_data[1]}' WHERE user_id={from_id}")
                return message.reply_html(
                    text="📌 پسورد جدید را وارد کنید:\n\n"
                        "⚠️ حداکثر 15 رقم می تواند باشد",
                    reply_to_message_id=message_id,
                    reply_markup={'resize_keyboard': True, 'keyboard': [[{'text': utl.menu_var}]]}
                )
            if ex_data[1] == 'api_per_number':
                cs.execute(f"UPDATE {utl.users} SET step='{ex_data[0]};{ex_data[1]}' WHERE user_id={from_id}")
                return message.reply_html(
                    text="📌 در هر API چند اکانت ثبت شود؟\n\n"
                        "- هر چقدر تعداد کمتر باشد دیلیتی کمتر خواهد بود (کمترین مقدار: 1)\n\n"
                        "- میتونید از API های اکانت های دیگر هم استفاده کنید (لازم نیست حتما API که وارد می کنید مال اکانتی باشه که در ربات لاگین می کنید)\n\n"
                        "توصیه ما: 5 ارسال\n\n"
                        "‏- API را باید از سایت تلگرام تهیه کنید:\n"
                        "https://my.telegram.org/auth\n\n"
                        "آموزش دریافت api از تلگرام:\n"
                        "https://www.youtube.com/watch?v=po3VVpwJHXY",
                    reply_to_message_id=message_id,
                    disable_web_page_preview=True,
                    reply_markup={'resize_keyboard': True, 'keyboard': [[{'text': utl.menu_var}]]}
                )
            if ex_data[1] == 'send_per_h':
                cs.execute(f"UPDATE {utl.users} SET step='{ex_data[0]};{ex_data[1]}' WHERE user_id={from_id}")
                return message.reply_html(
                    text="📌 هنگام ایجاد سفارش، هر اکانت چند ارسال انجام دهد؟\n\n"
                        "- تعداد 12 تا 18 خوب و حداکثر 28\n"
                        "- توصیه ما: 16 ارسال",
                    reply_to_message_id=message_id,
                    reply_markup={'resize_keyboard': True, 'keyboard': [[{'text': utl.menu_var}]]}
                )
            if ex_data[1] == 'limit_per_h':
                cs.execute(f"UPDATE {utl.users} SET step='{ex_data[0]};{ex_data[1]}' WHERE user_id={from_id}")
                return message.reply_html(
                    text="📌 وقتی اکانت یک سفارش را انجام داد، چه مدت استراحت کند؟\n\n"
                        "- اگر غیرفعال کنید احتمال اسپم شدن و دیلتی زیاد خواهد بود\n"
                        "- توصیه ما: 24 ساعت\n\n"
                        "❕ مقدار با برحسب ساعت و برای غیرفعال کردن 0 را ارسال کنید",
                    reply_to_message_id=message_id,
                    reply_markup={'resize_keyboard': True, 'keyboard': [[{'text': utl.menu_var}]]}
                )
            if ex_data[1] == 'change_pass' or ex_data[1] == 'exit_session' or ex_data[1] == 'is_change_profile' or ex_data[1] == 'is_set_username' or ex_data[1] == 'inbox_listen':
                if ex_data[1] == 'inbox_listen':
                    # toggle global inbox listening (0 = enabled, 1 = disabled)
                    row_admin['disable_inbox'] = 1 - int(row_admin.get('disable_inbox', 0))
                    cs.execute(f"UPDATE {utl.admin} SET disable_inbox=%s", (row_admin['disable_inbox'],))
                else:
                    row_admin[ex_data[1]] = 1 - row_admin[ex_data[1]]
                    cs.execute(f"UPDATE {utl.admin} SET {ex_data[1]}={row_admin[ex_data[1]]}")
            return message.edit_reply_markup(
                reply_markup={'inline_keyboard': [
                    [{'text': f"📝 در هر API چند اکانت ثبت شود: {row_admin['api_per_number']} اکانت",'callback_data': "settings;api_per_number"}],
                    [{'text': f"📝 ارسال هر اکانت در هر استفاده: {row_admin['send_per_h']} ارسال",'callback_data': "settings;send_per_h"}],
                    [{'text': (f"📝 استفاده اکانت هر چند ساعت: " + (f"{int(row_admin['limit_per_h'] / 3600)} ساعت" if row_admin['limit_per_h'] > 0 else "غیرفعال ❌")),'callback_data': "settings;limit_per_h"}],
                    [{'text': f"🔐 رمز دو مرحله ای: " + (row_admin['account_password'] if row_admin['account_password'] is not None else "ثبت نشده") + "",'callback_data': "settings;account_password"}],
                    [{'text': ("تنظیم / تغییر رمز دو مرحله ای: " + ("فعال ✅" if row_admin['change_pass'] > 0 else "غیرفعال ❌")),'callback_data': "settings;change_pass"}],
                    [{'text': ("خروج از بقیه سشن ها: " + ("فعال ✅" if row_admin['exit_session'] > 0 else "غیرفعال ❌")),'callback_data': "settings;exit_session"}],
                    [{'text': ("تنظیم نام، بیو و پروفایل: " + ("فعال ✅" if row_admin['is_change_profile'] > 0 else "غیرفعال ❌")),'callback_data': "settings;is_change_profile"}],
                    [{'text': ("تنظیم یوزرنیم: " + ("فعال ✅" if row_admin['is_set_username'] > 0 else "غیرفعال ❌")),'callback_data': "settings;is_set_username"}],
                    [{'text': ("شنود پیام‌ها: " + ("غیرفعال ❌" if row_admin.get('disable_inbox', 0) > 0 else "فعال ✅")), 'callback_data': "settings;inbox_listen"}],
                ]}
            )
        # Manual send flow from inbox: select account -> provide target -> provide message
        if ex_data[0] == 'inbox_manual':
            # show list of accounts to send from
            cs.execute(f"SELECT * FROM {utl.mbots} WHERE user_id IS NOT NULL ORDER BY id DESC")
            rows = cs.fetchall()
            if not rows:
                return query.answer(text="❌ هیچ اکانتی یافت نشد", show_alert=True)
            kb = []
            for r in rows:
                label = r.get('phone') or str(r.get('id'))
                kb.append([{'text': label, 'callback_data': f'inbox_manual_mbot;{r["id"]}'}])
            kb.append([{'text': 'بازگشت', 'callback_data': 'inbox_menu;new'}])
            try:
                return message.edit_text(text='📤 انتخاب اکانت برای ارسال دستی:', reply_markup={'inline_keyboard': kb})
            except Exception:
                return query.answer()

        if ex_data[0] == 'inbox_manual_mbot':
            # callback_data: inbox_manual_mbot;<mbot_id>
            try:
                mbot_id = int(ex_data[1])
            except Exception:
                return query.answer(text="❌ شناسه اکانت نامعتبر", show_alert=True)
            cs.execute(f"SELECT * FROM {utl.mbots} WHERE id=%s", (mbot_id,))
            row_m = cs.fetchone()
            if row_m is None:
                return query.answer(text="❌ اکانت یافت نشد", show_alert=True)
            # set user step to expect target id/username
            cs.execute(f"UPDATE {utl.users} SET step=%s WHERE user_id=%s", (f"manual_send_target;{mbot_id}", from_id))
            try:
                return message.edit_text(text='📌 لطفا آیدی یا یوزرنیم مقصد را ارسال کنید (مثلاً @username یا عدد).', reply_markup={'inline_keyboard': [[{'text': 'بازگشت', 'callback_data': 'inbox_menu;new'}]]})
            except Exception:
                return query.answer()
        if ex_data[0] == 'change_status':
            cs.execute(f"SELECT * FROM {utl.orders} WHERE id={int(ex_data[1])}")
            row_orders = cs.fetchone()
            if row_orders is None:
                query.answer(text="❌ سفارش یافت نشد", show_alert=True)
                return message.delete()
            if ex_data[2] == '2':
                if row_orders['status'] == 1:
                    if len(ex_data) == 3:
                        return message.edit_reply_markup(
                            reply_markup={'inline_keyboard': [
                                [{'text': 'آیا سفارش پایان یابد؟', 'callback_data': "nazan"}],
                                [{'text': '❌ نخیر ❌', 'callback_data': f"update;{row_orders['id']}"},{'text': '✅ بله ✅', 'callback_data': f"{ex_data[0]};{ex_data[1]};2;1"}]
                            ]}
                        )
                    if ex_data[3] == '1':
                        row_orders['status'] = 2
                        utl.end_order(cs, f"{directory}/files/exo_{row_orders['id']}_r.txt", row_orders)
            return message.edit_reply_markup(
                reply_markup={'inline_keyboard': [
                    [{'text': utl.status_orders[row_orders['status']], 'callback_data': "nazan"}],
                    [{'text': '🔄 بروزرسانی 🔄', 'callback_data': f"update;{row_orders['id']}"}]
                ]}
            )    
        if ex_data[0] == "analyze":
            cs.execute(f"SELECT * FROM {utl.egroup} WHERE id={int(ex_data[1])}")
            row_egroup = cs.fetchone()
            if row_egroup is None:
                return query.answer(text="❌ آنالیز یافت نشد", show_alert=True)
            
            cs.execute(f"UPDATE {utl.egroup} SET status=2 WHERE id={row_egroup['id']}")
            return message.edit_reply_markup(
                reply_markup={'inline_keyboard': [[{'text': "در حال اتمام ...",'callback_data': "nazan"}]]}
            )
        if ex_data[0] == "status_analyze":
            cs.execute(f"SELECT * FROM {utl.orders} WHERE WHERE id={int(ex_data[1])}")
            row_orders = cs.fetchone()
            if row_orders is None:
                return query.answer(text="❌ سفارش یافت نشد", show_alert=True)
            
            cs.execute(f"UPDATE {utl.orders} SET status_analyze=2 WHERE id={row_orders['id']}")
            return message.edit_reply_markup(
                reply_markup={'inline_keyboard': [[{'text': "در حال اتمام ...",'callback_data': "nazan"}]]}
            )
        if ex_data[0] == 'update':
            cs.execute(f"SELECT * FROM {utl.orders} WHERE id={int(ex_data[1])}")
            row_orders = cs.fetchone()
            if row_orders is None:
                return query.answer(text="❌ سفارش یافت نشد", show_alert=True)
            
            if row_orders['group_link'] is not None:
                output = f"\n🆔 <code>{row_orders['group_id']}</code>\n"
                output += f"🔗 {row_orders['group_link']}\n\n"
            else:
                output = "از طریق لیست انجام شده\n\n"
            if row_orders['cats'] is None:
                cats = "پشتیبانی نمی شود"
            else:
                where = ""
                cats = row_orders['cats'].split(",")
                for category in cats:
                    where += f"id={int(category)} OR "
                where = where[0:-4]
                cats = ""
                cs.execute(f"SELECT * FROM {utl.cats} WHERE {where}")
                result = cs.fetchall()
                for row in result:
                    cats += f"{row['name']},"
                cats = cats[0:-1]
            try:
                return message.edit_text(
                    text=f"اطلاعات گروه: {output}"
                        f"👤 ارسال شده / درخواستی: [{row_orders['count_done']:,} / {row_orders['count']:,}]\n"
                        f"👤 در حال بررسی / همه: [{row_orders['count_request']:,} / {row_orders['max_users']:,}]\n\n"
                        f"🔵 گزارش اکانت ها\n"
                        f"      استفاده شده: {row_orders['count_acc']:,}\n"
                        f"      محدود شده: {row_orders['count_restrict']:,}\n"
                        f"      ریپورت شده: {row_orders['count_report']:,}\n"
                        f"      از دست رفته: {row_orders['count_accout']:,}\n\n"
                        f"🔴 گزارش درخواست های ارسال\n"
                        f"      خطا های اسپم: {row_orders['count_usrspam']:,}\n"
                        f"      یوزرنیم اشتباه: {row_orders['count_userincorrect']:,}\n"
                        f"      اکانت های محدود: {row_orders['count_restrict_error']:,}\n"
                        f"      خطا های دیگر: {row_orders['count_other_errors']:,}\n\n"
                        f"🟣 دسته بندی ها: {cats}\n"
                        f"🟣 تعداد ارسال هر اکانت: {row_orders['send_per_h']:,}\n\n"
                        f"📥 خروجی کاربران باقی مانده: /exo_{row_orders['id']}_r\n"
                        f"📥 خروجی کاربران منتقل شده: /exo_{row_orders['id']}_m\n"
                        "➖➖➖➖➖➖\n"
                        f"📅️ ایجاد: {jdatetime.datetime.fromtimestamp(row_orders['created_at']).astimezone(datetime.timezone(datetime.timedelta(hours=3, minutes=30))).strftime('%Y/%m/%d %H:%M:%S')}\n"
                        f"📅️ بروزرسانی: {jdatetime.datetime.fromtimestamp(row_orders['updated_at']).astimezone(datetime.timezone(datetime.timedelta(hours=3, minutes=30))).strftime('%Y/%m/%d %H:%M:%S')}\n"
                        f"📅 الان: {jdatetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=3, minutes=30))).strftime('%Y/%m/%d %H:%M:%S')}",
                    parse_mode='HTML',
                    disable_web_page_preview=True,
                    reply_markup={'inline_keyboard': [
                        [{'text': utl.status_orders[row_orders['status']], 'callback_data': (f"change_status;{row_orders['id']};2" if row_orders['status'] == 1 else "nazan")}],
                        [{'text': '🔄 بروزرسانی 🔄', 'callback_data': f"update;{row_orders['id']}"}]
                    ]}
                )
            except telegram.error.BadRequest as e:
                if 'Message is not modified' in str(e):
                    # avoid noisy exception when content/markup are unchanged
                    return query.answer(text="✅ بروزرسانی انجام شد", show_alert=False)
                raise
        if ex_data[0] == 'gc':
            if ex_data[1] == '1':
                cs.execute(f"SELECT * FROM {utl.mbots} WHERE status=0")
                result = cs.fetchall()
                if not result:
                    return query.answer(text="❌ هیچ اکانتی یافت نشد", show_alert=True)
                
                for row_mbots in result:
                    try:
                        cs.execute(f"DELETE FROM {utl.mbots} WHERE id={row_mbots['id']}")
                        os.remove(f"{directory}/sessions/{row_mbots['uniq_id']}.session")
                    except:
                        pass
                return message.reply_html(text=f"✅ {len(result)} اکانت لاگ اوت شده حذف شدند")


def private_process(update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
    bot = context.bot
    message = update.message
    from_id = message.from_user.id
    chat_id = message.chat.id
    message_id = message.message_id
    text = message.text if message.text else ""
    if message.text:
        txtcap = message.text
    elif message.caption:
        txtcap = message.caption
    else:
        txtcap = ""
    ex_text = text.split('_')
    timestamp = int(time.time())

    cs = utl.Database()
    cs = cs.data()

    cs.execute(f"SELECT * FROM {utl.admin}")
    row_admin = cs.fetchone()
    cs.execute(f"SELECT * FROM {utl.users} WHERE user_id={from_id}")
    row_user = cs.fetchone()
    if row_user is None:
        uniq_id = utl.unique_id()
        cs.execute(f"INSERT INTO {utl.users} (user_id,status,step,created_at,uniq_id) VALUES ({from_id},0,'start',{timestamp},'{uniq_id}')")
        cs.execute(f"SELECT * FROM {utl.users} WHERE user_id={from_id}")
        row_user = cs.fetchone()
    ex_step = row_user['step'].split(';')
    
    if from_id in utl.admins or row_user['status'] == 1:
        # Allow the main-menu button to work from any step: reset to 'start'
        # This prevents getting stuck in a sub-step when the user taps the
        # keyboard's "🏛 منو اصلی" button.
        try:
            if text == utl.menu_var:
                cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                return user_panel(message=message)
        except Exception:
            # If anything goes wrong resetting the step, log and continue so
            # the existing handlers can provide an error message rather than
            # leaving the user stuck.
            try:
                log = globals().get('logger')
                if log:
                    log.exception('Failed to reset user step on menu_var for user=%s', from_id)
            except Exception:
                pass
        # Treat /start and /panel as global safety commands: always reset step and show panel.
        # Users can still send literal '/start' inside an input flow if they really need to,
        # but in practice admins expect /start to recover the bot UI when something got stuck.
        if text == '/start' or text == '/panel':
            # Allow literal '/start' to be treated as message content when the user is
            # actively composing a manual send or is in the create_order:get_message flow.
            # In all other cases treat it as a global reset that shows the panel.
            try:
                allow_as_content = False
                if ex_step and len(ex_step) > 0:
                    if ex_step[0] == 'manual_send_msg':
                        allow_as_content = True
                    elif ex_step[0] == 'create_order' and len(ex_step) > 2 and ex_step[2] == 'get_message':
                        allow_as_content = True
                if allow_as_content:
                    # Let the downstream handler treat the message as content.
                    pass
                else:
                    # Global reset behavior
                    cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                    user_panel(message=message)
                    # cleanup any transient order-in-progress
                    cs.execute(f"DELETE FROM {utl.orders} WHERE user_id={from_id} AND status=0")
                    return
            except Exception:
                # On unexpected error deciding, fall back to safe reset behavior.
                try:
                    cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                    user_panel(message=message)
                    cs.execute(f"DELETE FROM {utl.orders} WHERE user_id={from_id} AND status=0")
                except Exception:
                    pass
                return
        if text == '/restart':
            info_msg = message.reply_html(text="در حال بررسی ...")
            # spawn run.py in background so the bot process is not blocked
            subprocess.Popen([utl.python_version, f"{directory}/run.py"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
            return info_msg.edit_text(text="✅ انجام شد")
        # Show inline inbox menu when admin presses the reply-keyboard button
        if text == '📩 پیام‌ها':
            # three inline choices: new / read / all
            try:
                # compute total new messages
                cs.execute(f"SELECT COUNT(*) as cnt FROM {utl.inbox} WHERE processed=0")
                row_tmp = cs.fetchone()
                total_new = row_tmp['cnt'] if row_tmp is not None else 0
            except Exception:
                total_new = 0

            new_label = f" 📬 جدید ها  ({total_new})" if total_new else "🆕 جدیدها"
            kb = [
                [{'text': new_label, 'callback_data': 'inbox_menu;new'}],
                [{'text': '📖 خوانده شده‌ها', 'callback_data': 'inbox_menu;read'}],
                [{'text': '📨 همه پیام‌ها', 'callback_data': 'inbox_menu;all'}]
                , [{'text': '✉️ ارسال دستی', 'callback_data': 'inbox_manual'}]
            ]
            return message.reply_text(text='📩 بخش پیام‌ها - یک گزینه را انتخاب کنید:', reply_markup={'inline_keyboard': kb})
        if ex_step[0] == 'set_cache':
            if not message.forward_from_chat:
                return message.reply_html(text="❌ یک پست از کانال فوروارد کنید", reply_to_message_id=message_id)
            if not message.forward_from_chat.username:
                return message.reply_html(text="❌ کانال باید عمومی باشد", reply_to_message_id=message_id)
            if bot.get_chat_member(chat_id=message.forward_from_chat.id, user_id=utl.bot_id).status == "left":
                return message.reply_html(text="❌ ربات باید در کانال ادمین باشد", reply_to_message_id=message_id)
            
            cs.execute(f"UPDATE {utl.admin} SET cache='{message.forward_from_chat.username}'")
            cs.execute(f"UPDATE {utl.users} SET step='panel' WHERE user_id={from_id}")
            return user_panel(message=message, text="✅ کانال کش با موفقیت ثبت شد", reply_to_message_id=message_id)
        if row_admin['cache'] is None or text == "📣 کانال کش":
            cs.execute(f"UPDATE {utl.users} SET step='set_cache;none' WHERE user_id={from_id}")
            return message.reply_html(
                text="برای ثبت کانال کش یک پست از کانال به اینجا فوروارد کنید:\n\n"
                    "❕ پیام هایی که قرار است به کاربران ارسال شود ابتدا در این کانال ذخیره می شوند، تا ربات موقع ارسال به آن ها دسترسی داشته باشد",
                reply_markup={'resize_keyboard': True,'keyboard': [[{'text': utl.menu_var}]]}
            )
        if ex_step[0] == 'info_user':
            try:
                user_id = int(text)
            except:
                return message.reply_html(text="❌ ورودی نامعتبر", reply_to_message_id=message_id)
            cs.execute(f"SELECT * FROM {utl.users} WHERE user_id={user_id}")
            row_user_select = cs.fetchone()
            if row_user_select is None:
                return message.reply_html(
                    text="❌ آیدی عددی اشتباه است\n\n"
                        "❕ دقت کنید که کاربر قبلا باید ربات را استارت کرده باشد",
                    reply_to_message_id=message_id
                )
            admin_status = 0 if row_user_select['status'] == 1 else 1
            message.reply_html(
                text=f"کاربر <a href='tg://user?id={row_user_select['user_id']}'>{row_user_select['user_id']}</a>",
                reply_markup={'inline_keyboard': [
                    [{'text': "ارسال پیام",'callback_data': f"d;{row_user_select['user_id']};sendmsg"}],
                    [{'text': ('ادمین ✅' if row_user_select['status'] == 1 else 'ادمین ❌'), 'callback_data': f"d;{row_user_select['user_id']};{admin_status}"}]
                ]}
            )
            cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
            return user_panel(message=message)
        if ex_step[0] == 'sendmsg':
            cs.execute(f"SELECT * FROM {utl.users} WHERE user_id={int(ex_step[1])}")
            row_user_select = cs.fetchone()
            if row_user_select is None:
                return message.reply_html(text="❌ کاربر یافت نشد", reply_to_message_id=message_id)
            if not message.text and not message.photo and message.video and message.audio and message.voice and message.document:
                return message.reply_html(text="⛔️ پیام پشتیبانی نمی شود", reply_to_message_id=message_id)
            try:
                content = f"📧️ پیام از طرف پشتیبانی\n——————————————————\n{txtcap}"
                if message.text:
                    bot.send_message(chat_id=row_user_select['user_id'], text=content, parse_mode='HTML', disable_web_page_preview=True)
                elif message.photo:
                    bot.send_photo(chat_id=row_user_select['user_id'], caption=content, photo=message.photo[len(message.photo) - 1].file_id, parse_mode='HTML')
                elif message.video:
                    bot.send_video(chat_id=row_user_select['user_id'], video=message.video.file_id, caption=content, parse_mode='HTML')
                elif message.audio:
                    bot.send_audio(chat_id=row_user_select['user_id'], audio=message.audio.file_id, caption=content, parse_mode='HTML')
                elif message.voice:
                    bot.send_voice(chat_id=row_user_select['user_id'], voice=message.voice.file_id, caption=content, parse_mode='HTML')
                elif message.document:
                    bot.send_document(chat_id=row_user_select['user_id'], document=message.document.file_id, caption=content, parse_mode='HTML')
                cs.execute(f"UPDATE {utl.users} SET step='panel' WHERE user_id={from_id}")
                return user_panel(message=message, text="✅ پیام با موفقیت ارسال شد", reply_to_message_id=message_id)
            except:
                return message.reply_html(text="❌ خطای ناشناخته، مجدد تلاش کنید", reply_to_message_id=message_id)
        if ex_step[0] == 'add_api':
            try:
                ex_nl_text = text.split("\n")
                if len(ex_nl_text) != 2 or len(ex_nl_text[0]) > 50 or len(ex_nl_text[1]) > 200:
                    return message.reply_html(text="❌ ورودی اشتباه است", reply_to_message_id=message_id)
                if not re.findall('^[0-9]*$', ex_nl_text[0]):
                    return message.reply_html(text="‏❌ api id اشتیاه است", reply_to_message_id=message_id)
                if not re.findall('^[0-9-a-z-A-Z]*$', ex_nl_text[1]):
                    return message.reply_html(text="‏❌ api hash اشتیاه است", reply_to_message_id=message_id)
                
                api_id = ex_nl_text[0]
                api_hash = ex_nl_text[1]
                cs.execute(f"SELECT * FROM {utl.apis} WHERE api_id='{api_id}' OR api_hash='{api_hash}'")
                if cs.fetchone() is not None:
                    return message.reply_html(text="❌ این API قبل افزوده شده است", reply_to_message_id=message_id)
                
                cs.execute(f"INSERT INTO {utl.apis} (api_id,api_hash) VALUES ('{api_id}','{api_hash}')")
                return message.reply_html(
                    text="✅ با موفقیت اضافه شده\n\n"
                        "مورد دیگری اضافه کنید:",
                    reply_to_message_id=message_id,
                    reply_markup={'resize_keyboard': True, 'keyboard': [[{'text': utl.menu_var}]]}
                )
            except:
                return message.reply_html(text="❌ ورودی اشتباه", reply_to_message_id=message_id)
        if ex_step[0] == 'create_cat':
            cs.execute(f"SELECT * FROM {utl.cats} WHERE name='{text}'")
            row_cats = cs.fetchone()
            if row_cats is not None:
                return message.reply_html(text="❌ دسته بندی قبلا ایجاد شده است", reply_to_message_id=message_id)
            else:
                cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                cs.execute(f"INSERT INTO {utl.cats} (name) VALUES ('{text}')")
                return user_panel(message=message, text="✅ با موفقیت ایجاد شد", reply_to_message_id=message_id)
        if ex_step[0] == 'set_cat':
            cs.execute(f"SELECT * FROM {utl.mbots} WHERE id={int(ex_step[1])}")
            row_mbots = cs.fetchone()
            if row_mbots is None:
                return message.reply_html(text="❌ اکانت یافت نشد", reply_to_message_id=message_id)
            cs.execute(f"SELECT * FROM {utl.cats} WHERE name='{text}'")
            row_cats = cs.fetchone()
            if row_cats is None:
                return message.reply_html(text="❌ دسته بندی یافت نشد", reply_to_message_id=message_id)
            
            cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
            cs.execute(f"UPDATE {utl.mbots} SET cat_id={row_cats['id']} WHERE id={row_mbots['id']}")
            return message.reply_html(
                text="✅ با موفقیت بروزرسانی شد",
                reply_markup={'resize_keyboard': True, 'keyboard': [[{'text': utl.menu_var}]]}
            )
        if ex_step[0] == 'reply_inbox':
            # ex_step: reply_inbox;inbox_id;mbot_id
            try:
                inbox_id = int(ex_step[1])
                mbot_id = int(ex_step[2])
            except:
                cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                return message.reply_html(text="❌ شناسه نامعتبر", reply_to_message_id=message_id)
            cs.execute(f"SELECT * FROM {utl.inbox} WHERE id={inbox_id}")
            row_in = cs.fetchone()
            if row_in is None:
                cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                return message.reply_html(text="❌ پیام یافت نشد", reply_to_message_id=message_id)
            # validate message content
            if not message.text and not message.photo and message.video and message.audio and message.voice and message.document:
                return message.reply_html(text="⛔️ پیام پشتیبانی نمی شود", reply_to_message_id=message_id)
            try:
                # prepare content
                content = txtcap
                target_id = row_in['from_id'] if row_in['from_id'] is not None else None
                target_username = row_in['from_username'] if row_in['from_username'] is not None else None
                logger.info('Admin %s replying to inbox.id=%s via mbot=%s target_id=%s target_username=%s len_text=%s', from_id, inbox_id, mbot_id, target_id, target_username, (len(content) if content else 0))
                outbox_insert = admin_reply_queue(mbot_id, target_id=target_id, target_username=target_username, text=content, reply_to_inbox_id=inbox_id)
                cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                if outbox_insert:
                    try:
                        # mark the original inbox message as processed (read) when admin replied
                        cs.execute(f"UPDATE {utl.inbox} SET processed=1 WHERE id=%s", (inbox_id,))
                    except Exception:
                        pass
                    try:
                        # if we have an inserted id, include it in confirmation
                        out_id = outbox_insert if isinstance(outbox_insert, int) else None
                        msg_text = "✅ پاسخ در صف ارسال قرار گرفت و پیام علامت خوانده شده شد"
                        if out_id:
                            msg_text += f" (outbox_id={out_id})"
                        return user_panel(message=message, text=msg_text, reply_to_message_id=message_id)
                    except Exception:
                        return user_panel(message=message, text="✅ پاسخ در صف ارسال قرار گرفت و پیام علامت خوانده شده شد", reply_to_message_id=message_id)
                else:
                    return message.reply_text(text="❌ خطا در افزودن به صف", reply_to_message_id=message_id)
            except Exception as e:
                cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                return message.reply_html(text=f"❌ خطا: {e}", reply_to_message_id=message_id)
        # Manual send flow - handle target input and message input
        if ex_step[0] == 'manual_send_target':
            # ex_step: manual_send_target;{mbot_id}
            try:
                mbot_id = int(ex_step[1])
            except Exception:
                cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                return message.reply_html(text="❌ شناسه اکانت نامعتبر", reply_to_message_id=message_id)
            # expect text like @username or numeric id
            if not text:
                return message.reply_html(text="❌ لطفا آیدی یا یوزرنیم مقصد را ارسال کنید", reply_to_message_id=message_id)
            t = text.strip()
            step_payload = None
            # numeric id
            if re.match(r"^\d+$", t):
                step_payload = f"id_{t}"
            else:
                # allow @username or username without @
                uname = t if t.startswith('@') else ('@' + t)
                step_payload = f"usr_{uname.lstrip('@')}"
            # set next step to receive message
            cs.execute(f"UPDATE {utl.users} SET step=%s WHERE user_id=%s", (f"manual_send_msg;{mbot_id};{step_payload}", from_id))
            return message.reply_html(text="📌 حالا پیام یا فایل را ارسال کنید (متن، عکس، ویدئو یا فایل).", reply_to_message_id=message_id, reply_markup={'resize_keyboard': True, 'keyboard': [[{'text': utl.menu_var}]]})

        if ex_step[0] == 'manual_send_msg':
            # ex_step: manual_send_msg;{mbot_id};{id_xxx|usr_xxx}
            try:
                mbot_id = int(ex_step[1])
                payload = ex_step[2]
            except Exception:
                cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                return message.reply_html(text="❌ خطا در پارس کردن مرحله", reply_to_message_id=message_id)
            # validate supported content
            if not (message.text or message.photo or message.video or message.audio or message.voice or message.document):
                return message.reply_html(text="⛔️ این نوع پیام پشتیبانی نمی شود", reply_to_message_id=message_id)
            # determine target
            target_id = None
            target_username = None
            try:
                if payload.startswith('id_'):
                    target_id = int(payload.replace('id_', ''))
                elif payload.startswith('usr_'):
                    uname = payload.replace('usr_', '')
                    # ensure it starts with @ when sending via admin_reply_queue we store username without @ or with @? we'll send as @username
                    target_username = '@' + uname.lstrip('@')
            except Exception:
                cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                return message.reply_html(text="❌ شناسه مقصد نامعتبر", reply_to_message_id=message_id)
            # prepare content
            content = txtcap
            # enqueue to outbox via admin_reply_queue
            logger.info('Admin %s manual-send via mbot=%s target_id=%s target_username=%s len_text=%s', from_id, mbot_id, target_id, target_username, (len(content) if content else 0))
            outbox_insert = admin_reply_queue(mbot_id, target_id=target_id, target_username=target_username, text=content)
            cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
            if outbox_insert:
                try:
                    out_id = outbox_insert if isinstance(outbox_insert, int) else None
                    msg_text = '✅ پیام در صف قرار گرفت و ارسال خواهد شد'
                    if out_id:
                        msg_text += f' (outbox_id={out_id})'
                    return user_panel(message=message, text=msg_text, reply_to_message_id=message_id)
                except Exception:
                    return user_panel(message=message, text='✅ پیام در صف قرار گرفت و ارسال خواهد شد', reply_to_message_id=message_id)
            else:
                return message.reply_html(text="❌ خطا در افزودن به صف", reply_to_message_id=message_id)
        if ex_step[0] == 'analyze':
            if ex_step[1] == 'type':
                if text == 'کاربران':
                    cs.execute(f"UPDATE {utl.users} SET step='analyze;users;link' WHERE user_id={from_id}")
                    return message.reply_html(
                        text="لینک گروه را ارسال کنید:",
                        reply_markup={'resize_keyboard': True, 'keyboard': [[{'text': utl.menu_var}]]}
                    )
                if text == 'پیام ها':
                    cs.execute(f"UPDATE {utl.users} SET step='analyze;messages;link' WHERE user_id={from_id}")
                    return message.reply_html(
                        text="لینک گروه را ارسال کنید:",
                        reply_markup={'resize_keyboard': True, 'keyboard': [[{'text': utl.menu_var}]]}
                    )
                return message.reply_html(text="⛔️ از منو انتخاب کنید", reply_to_message_id=message_id)
            if ex_step[1] == 'users':
                if ex_step[2] == 'link':
                    cs.execute(f"SELECT * FROM {utl.mbots} WHERE status>0 ORDER BY RAND()")
                    row_mbots = cs.fetchone()
                    if row_mbots is None:
                        return message.reply_html(text="❌ هیچ اکانتی یافت نشد", reply_to_message_id=message_id)
                    uniq_id = utl.unique_id()
                    try:
                        int(text)
                        cs.execute(f"INSERT INTO {utl.egroup} (type,user_id,chat_id,status,created_at,updated_at,uniq_id) VALUES (0,{from_id},'{text}',0,{timestamp},{timestamp},'{uniq_id}')")
                    except:
                        text = text.replace("/+", "/joinchat/")
                        cs.execute(f"INSERT INTO {utl.egroup} (type,user_id,link,status,created_at,updated_at,uniq_id) VALUES (0,{from_id},'{text}',0,{timestamp},{timestamp},'{uniq_id}')")
                    cs.execute(f"SELECT * FROM {utl.egroup} WHERE uniq_id='{uniq_id}'")
                    row_egroup = cs.fetchone()
                    if row_egroup is None:
                        return message.reply_html(text="❌ خطای ناشناخته، مجدد تلاش کنید", reply_to_message_id=message_id)
                    cs.execute(f"UPDATE {utl.users} SET step='{ex_step[0]};{ex_step[1]};account;{row_egroup['id']}' WHERE user_id={from_id}")
                    return message.reply_html(
                        text="آیدی عددی اکانت رو ارسال کنید:",
                        reply_markup={'resize_keyboard': True, 'keyboard': [
                            [{'text': "اکانت رندوم"}],
                            [{'text': utl.menu_var}]
                        ]}
                    )
                elif ex_step[2] == 'account':
                    cs.execute(f"SELECT * FROM {utl.egroup} WHERE id={int(ex_step[3])}")
                    row_egroup = cs.fetchone()
                    if row_egroup is None:
                        return message.reply_html(text="❌ خطای ناشناخته، مجدد تلاش کنید", reply_to_message_id=message_id)
                    if text == "اکانت رندوم":
                        cs.execute(f"SELECT * FROM {utl.mbots} WHERE status>0 ORDER BY RAND()")
                    else:
                        cs.execute(f"SELECT * FROM {utl.mbots} WHERE status>0 AND user_id={int(text)}")
                    row_mbots = cs.fetchone()
                    if row_mbots is None:
                        return message.reply_html(text="❌ هیچ اکانتی یافت نشد", reply_to_message_id=message_id)
                    
                    cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                    info_msg = message.reply_html(text="در حال اتصال ...", reply_to_message_id=message_id)
                    # run analyzer in background to avoid blocking the bot
                    subprocess.Popen([utl.python_version, f"{directory}/tl_analyze.py", row_mbots['uniq_id'], str(from_id), str(row_egroup['id']), 'users', str(info_msg.message_id)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
                    user_panel(message=message)
                    return info_msg.delete()
            if ex_step[1] == 'messages':
                if ex_step[2] == 'link':
                    cs.execute(f"SELECT * FROM {utl.mbots} WHERE status>0 ORDER BY RAND()")
                    row_mbots = cs.fetchone()
                    if row_mbots is None:
                        return message.reply_html(text="❌ هیچ اکانتی یافت نشد", reply_to_message_id=message_id)
                    uniq_id = utl.unique_id()
                    try:
                        int(text)
                        cs.execute(f"INSERT INTO {utl.egroup} (type,user_id,chat_id,status,created_at,updated_at,uniq_id) VALUES (1,{from_id},'{text}',0,'{timestamp}','{timestamp}','{uniq_id}')")
                    except:
                        text = text.replace("/+", "/joinchat/")
                        cs.execute(f"INSERT INTO {utl.egroup} (type,user_id,link,status,created_at,updated_at,uniq_id) VALUES (1,{from_id},'{text}',0,'{timestamp}','{timestamp}','{uniq_id}')")
                    cs.execute(f"SELECT * FROM {utl.egroup} WHERE uniq_id='{uniq_id}'")
                    row_egroup = cs.fetchone()
                    if row_egroup is None:
                        return message.reply_html(text="❌ خطای ناشناخته، مجدد تلاش کنید", reply_to_message_id=message_id)
                    
                    cs.execute(f"UPDATE {utl.users} SET step='{ex_step[0]};{ex_step[1]};account;{row_egroup['id']}' WHERE user_id={from_id}")
                    return message.reply_html(
                        text="آیدی عددی اکانت رو ارسال کنید:",
                        reply_markup={'resize_keyboard': True, 'keyboard': [
                            [{'text': "اکانت رندوم"}],
                            [{'text': utl.menu_var}]
                        ]}
                    )
                elif ex_step[2] == 'account':
                    cs.execute(f"SELECT * FROM {utl.egroup} WHERE id={int(ex_step[3])}")
                    row_egroup = cs.fetchone()
                    if row_egroup is None:
                        return message.reply_html(text="❌ خطای ناشناخته، مجدد تلاش کنید", reply_to_message_id=message_id)
                    if text == "اکانت رندوم":
                        cs.execute(f"SELECT * FROM {utl.mbots} WHERE status>0 ORDER BY RAND()")
                    else:
                        cs.execute(f"SELECT * FROM {utl.mbots} WHERE status>0 AND user_id={int(text)}")
                    row_mbots = cs.fetchone()
                    if row_mbots is None:
                        return message.reply_html(text="❌ هیچ اکانتی یافت نشد", reply_to_message_id=message_id)
                    
                    cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                    info_msg = message.reply_html(text="در حال اتصال ...", reply_to_message_id=message_id)
                    subprocess.Popen([utl.python_version, f"{directory}/tl_analyze.py", row_mbots['uniq_id'], str(from_id), str(row_egroup['id']), 'messages', str(info_msg.message_id)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
                    user_panel(message=message)
                    return info_msg.delete()
        if ex_step[0] == 'settings':
            if ex_step[1] == 'account_password':
                if len(text) > 15:
                    return message.reply_html(text="❌ ورودی نامعتبر", reply_to_message_id=message_id)
                cs.execute(f"UPDATE {utl.admin} SET {ex_step[1]}='{text}'")
                cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                return user_panel(message=message, text="✅ با موفقیت بروزرسانی شد", reply_to_message_id=message_id)
            if ex_step[1] == 'api_per_number':
                try:
                    api_per_number = int(text)
                    if api_per_number < 1:
                        return message.reply_html(text="❌ ورودی نامعتبر", reply_to_message_id=message_id)
                except:
                    return message.reply_html(text="❌ ورودی نامعتبر", reply_to_message_id=message_id)
                cs.execute(f"UPDATE {utl.admin} SET {ex_step[1]}={api_per_number}")
                cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                return user_panel(message=message, text="✅ با موفقیت بروزرسانی شد", reply_to_message_id=message_id)
            if ex_step[1] == 'send_per_h':
                try:
                    send_per_h = int(text)
                    if send_per_h < 1:
                        return message.reply_html(text="❌ ورودی نامعتبر", reply_to_message_id=message_id)
                except:
                    return message.reply_html(text="❌ ورودی نامعتبر", reply_to_message_id=message_id)
                cs.execute(f"UPDATE {utl.admin} SET {ex_step[1]}={send_per_h}")
                cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                return user_panel(message=message, text="✅ با موفقیت بروزرسانی شد", reply_to_message_id=message_id)
            if ex_step[1] == 'limit_per_h':
                try:
                    limit_per_h = int(text) * 3600
                    if limit_per_h < 0:
                        return message.reply_html(text="❌ ورودی نامعتبر", reply_to_message_id=message_id)
                except:
                    return message.reply_html(text="❌ ورودی نامعتبر", reply_to_message_id=message_id)
                cs.execute(f"UPDATE {utl.admin} SET {ex_step[1]}={limit_per_h}")
                cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                return user_panel(message=message, text="✅ با موفقیت بروزرسانی شد", reply_to_message_id=message_id)
        if ex_step[0] == 'add_acc':
            cs.execute(f"SELECT * FROM {utl.mbots} WHERE id={int(ex_step[1])}")
            row_mbots = cs.fetchone()
            if row_mbots is None:
                return message.reply_html(text="❌ خطای ناشناخته، مجدد تلاش کنید", reply_to_message_id=message_id)
            if ex_step[2] == 'type':
                if text == 'شماره':
                    cs.execute(f"UPDATE {utl.users} SET step='{ex_step[0]};{row_mbots['id']};number;phone' WHERE user_id={from_id}")
                    return message.reply_html(
                        text="شماره را به هماره کد کشور وارد کنید:",
                        reply_markup={'resize_keyboard': True,'keyboard': [[{'text': utl.menu_var}]]}
                    )
                if text == 'سشن':
                    cs.execute(f"UPDATE {utl.users} SET step='{ex_step[0]};{row_mbots['id']};session' WHERE user_id={from_id}")
                    return message.reply_html(
                        text="فایل سشن تلتون را ارسال کنید:",
                        reply_markup={'resize_keyboard': True,'keyboard': [[{'text': utl.menu_var}]]}
                    )
                if text == 'زیپ':
                    cs.execute(f"UPDATE {utl.users} SET step='{ex_step[0]};{row_mbots['id']};zip' WHERE user_id={from_id}")
                    return message.reply_html(
                        text="فایل های سشن تلتون را داخل یک فایل زیپ ارسال کنید:",
                        reply_markup={'resize_keyboard': True,'keyboard': [[{'text': utl.menu_var}]]}
                    )
                return message.reply_html(text="⛔️ از منو انتخاب کنید", reply_to_message_id=message_id)
            if ex_step[2] == 'session':
                if not message.document or message.document.file_name[-8:] != ".session":
                    return message.reply_html(text="❌ فایل باید از نوع سشن تلتون باشد", reply_to_message_id=message_id)
                row_apis = utl.select_api(cs, row_admin['api_per_number'])
                if row_apis is None:
                    return message.reply_html(text="❌ ابتدا یک API اضافه کنید یا از تنظیمات گزینه اول را افزایش دهید", reply_to_message_id=message_id)
                try:
                    unique_id = utl.unique_id()
                    cs.execute(f"INSERT INTO {utl.mbots} (cat_id,creator_user_id,api_id,api_hash,status,created_at,uniq_id) VALUES (1,{from_id},'{row_apis['api_id']}','{row_apis['api_hash']}',0,{int(time.time())},'{unique_id}')")
                    cs.execute(f"SELECT * FROM {utl.mbots} WHERE uniq_id='{unique_id}'")
                    row_mbots_select = cs.fetchone()
                    if row_mbots_select is None:
                        return message.reply_html(text="❌ خطای ناشناخته، مجدد تلاش کنید", reply_to_message_id=message_id)
                    info_action = bot.get_file(message.document.file_id)
                    with open(f"{directory}/sessions/{row_mbots_select['uniq_id']}.session", "wb") as file:
                        file.write(requests.get(info_action.file_path).content)
                    info_msg = message.reply_html(text="در حال بررسی ...")
                    subprocess.Popen([utl.python_version, f"{directory}/tl_import.py", row_mbots_select['uniq_id']], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
                    cs.execute(f"SELECT * FROM {utl.mbots} WHERE id={row_mbots_select['id']}")
                    row_mbots_select = cs.fetchone()
                    if row_mbots_select is not None:
                        if row_mbots_select['status'] == 1:
                            return info_msg.edit_text(text=f"✅ ذخیره شد: <code>{row_mbots_select['phone']}</code>", parse_mode="html")
                        else:
                            cs.execute(f"DELETE FROM {utl.mbots} WHERE id={row_mbots_select['id']}")
                            return info_msg.edit_text(text=f"❕ قبلا اضافه شده: <code>{row_mbots_select['phone']}</code>", parse_mode="html")
                    else:
                        return info_msg.edit_text(text="❌ سشن معتبر نیست")
                except:
                    return message.reply_html(text="❌ خطای ناشناخته، مجدد تلاش کنید", reply_to_message_id=message_id)
            if ex_step[2] == 'zip':
                cs.execute(f"DELETE FROM {utl.mbots} WHERE creator_user_id={from_id} AND status=0 AND user_id IS NULL")
                if not message.document or message.document.file_name[-4:] != ".zip":
                    return message.reply_html(text="❌ فایل باید از نوع زیپ فایل", reply_to_message_id=message_id)
                try:
                    try:
                        shutil.rmtree(f"{directory}/import")
                    except:
                        pass
                    if not os.path.exists(f"{directory}/import"):
                        os.mkdir(f"{directory}/import")
                    info_msg = message.reply_html(text="در حال دانلود ...", reply_to_message_id=message_id)
                    info_action = bot.get_file(message.document.file_id)
                    with open(f"{directory}/file.zip", "wb") as file:
                        file.write(requests.get(info_action.file_path).content)
                    
                    info_msg.edit_text(text="در حال آنالیز ...")
                    with zipfile.ZipFile(f"{directory}/file.zip", 'r') as zObject:
                        zObject.extractall(path=f"{directory}/import")
                    os.remove(f"{directory}/file.zip")
                    
                    info_msg.edit_text(text="در حال انجام عملیات ...")
                    list_files = os.listdir(f"{directory}/import")
                    count_all = len(list_files)
                    count_import_success = count_import_failed = count_import_existed = 0
                    for file in list_files:
                        row_apis = utl.select_api(cs, row_admin['api_per_number'])
                        if row_apis is None:
                            message.reply_html(text="❌ ابتدا یک API اضافه کنید یا از تنظیمات گزینه اول را افزایش دهید", reply_to_message_id=message_id)
                            break
                        if file[-8:] == ".session":
                            try:
                                unique_id = utl.unique_id()
                                cs.execute(f"INSERT INTO {utl.mbots} (cat_id,creator_user_id,api_id,api_hash,status,created_at,uniq_id) VALUES (1,{from_id},'{row_apis['api_id']}','{row_apis['api_hash']}',0,{int(time.time())},'{unique_id}')")
                                cs.execute(f"SELECT * FROM {utl.mbots} WHERE uniq_id='{unique_id}'")
                                row_mbots = cs.fetchone()
                                with open(f"{directory}/import/{file}", "rb") as file:
                                    content = file.read()
                                with open(f"{directory}/sessions/{row_mbots['uniq_id']}.session", "wb") as file:
                                    file.write(content)
                                subprocess.Popen([utl.python_version, f"{directory}/tl_import.py", row_mbots['uniq_id']], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
                                cs.execute(f"SELECT * FROM {utl.mbots} WHERE id={row_mbots['id']}")
                                row_mbots = cs.fetchone()
                                if row_mbots is not None:
                                    if row_mbots['status'] == 1:
                                        count_import_success += 1
                                    else:
                                        count_import_existed += 1
                                        cs.execute(f"DELETE FROM {utl.mbots} WHERE id={row_mbots['id']}")
                                else:
                                    count_import_failed += 1
                            except:
                                pass
                            try:
                                info_msg.edit_text(
                                    text="در حال انجام عملیات ...\n"
                                        f"⏳ در حال بررسی: [{(count_import_success + count_import_failed + count_import_existed):,} / {count_all:,}]\n\n"
                                        f"✅ موفق: {count_import_success:,}\n"
                                        f"❌ ناموفق: {count_import_failed:,}\n"
                                        f"❕ قبلا اضافه شده: {count_import_existed:,}\n"
                                )
                            except:
                                pass
                    info_msg.reply_html(
                        text=f"عملیات پایان یافت: [{(count_import_success + count_import_failed + count_import_existed):,} / {count_all:,}]\n\n"
                            f"✅ موفق: {count_import_success:,}\n"
                            f"❌ ناموفق: {count_import_failed:,}\n"
                            f"❕ قبلا اضافه شده: {count_import_existed:,}\n"
                    )
                    try:
                        shutil.rmtree(f"{directory}/import")
                    except:
                        pass
                    return
                except Exception as e:
                    print(e)
                    return message.reply_html(text="❌ خطای ناشناخته، مجدد تلاش کنید", reply_to_message_id=message_id)
            if ex_step[2] == 'number':
                if ex_step[3] == 'phone':
                    phone = text.replace("+","").replace(" ","")
                    if not re.findall('^[0-9]*$', phone):
                        return message.reply_html(text="❌ شماره اشتباه است", reply_to_message_id=message_id)
                    
                    cs.execute(f"SELECT * FROM {utl.mbots} WHERE phone='{phone}' AND status>0")
                    row_mbots_select = cs.fetchone()
                    if row_mbots_select is not None:
                        return message.reply_html(text="❌ شماره قبلا اضافه شده است", reply_to_message_id=message_id)
                    # Use parameterized query and handle duplicate-key (race condition) gracefully
                    try:
                        cs.execute(f"UPDATE {utl.mbots} SET phone=%s WHERE id=%s", (phone, row_mbots['id']))
                    except Exception as e:
                        msg = str(e)
                        # MySQL duplicate key error contains 'Duplicate entry' in message
                        if 'Duplicate entry' in msg:
                            return message.reply_html(text="❌ این شماره قبلا به یک اکانت دیگر اختصاص داده شده", reply_to_message_id=message_id)
                        else:
                            return message.reply_html(text="❌ خطا در پایگاه داده، مجدد تلاش کنید", reply_to_message_id=message_id)

                    info_msg = message.reply_html(text="در حال اتصال ...", reply_to_message_id=message_id)
                    # spawn the account login helper in background so the bot remains responsive
                    try:
                        subprocess.Popen([utl.python_version, f"{directory}/tl_account.py", row_mbots['uniq_id'], str(from_id), str(info_msg.message_id)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
                    except Exception as e:
                        try:
                            info_msg.edit_text(text="❌ خطا در اجرای فرایند اتصال، مجدد تلاش کنید")
                        except Exception:
                            pass
                        return
                    # keep the info message so the background process can update it with progress/result
                    return info_msg
                if ex_step[3] == 'code':
                    try:
                        code = int(text)
                    except:
                        pass
                    return cs.execute(f"UPDATE {utl.mbots} SET code={code} WHERE id={row_mbots['id']}")
                if ex_step[3] == 'password':
                    return cs.execute(f"UPDATE {utl.mbots} SET password='{text}' WHERE id={row_mbots['id']}")
        if ex_step[0] == 'create_order':
            cs.execute(f"SELECT * FROM {utl.orders} WHERE id={int(ex_step[1])}")
            row_orders = cs.fetchone()
            if row_orders is None:
                return message.reply_html(text="❌ سفارش یافت نشد، مجدد تلاش کنید", reply_to_message_id=message_id)
            if ex_step[2] == 'category':
                if text == "⏩ بعدی":
                    if row_orders['cats'] is None:
                        return message.reply_html(text="❌ حداقل باید یک دسته بندی را انتخاب کنید", reply_to_message_id=message_id)
                    cs.execute(f"UPDATE {utl.users} SET step='{ex_step[0]};{ex_step[1]};type_send' WHERE user_id={from_id}")
                    return message.reply_html(
                        text="آیا می خواهید کاربران تکراری حدف شوند؟",
                        reply_markup={'resize_keyboard': True,'keyboard': [
                            [{'text': 'خیر'}, {'text': 'بله'}],
                            [{'text': utl.menu_var}]
                        ]}
                    )
                else:
                    cs.execute(f"SELECT * FROM {utl.cats} WHERE name='{text}'")
                    row_cats = cs.fetchone()
                    if row_cats is None:
                        return message.reply_html(text="❌ دسته بندی یافت نشد", reply_to_message_id=message_id)
                    cats = ""
                    if row_orders['cats'] is not None:
                        cats = row_orders['cats'].split(",")
                        for category in cats:
                            try:
                                if int(category) == row_cats['id']:
                                    return message.reply_html(text=f"❌ دسته بندی <b>{row_cats['name']}</b> قبلا انتخاب شده است", reply_to_message_id=message_id)
                            except:
                                pass
                        cats = f"{row_orders['cats']},{row_cats['id']}"
                    else:
                        cats = row_cats['id']
                    row_orders['cats'] = str(cats)
                    
                    where = ""
                    cats = row_orders['cats'].split(",")
                    for category in cats:
                        where += f"cat_id={int(category)} OR "
                    where = where[0:-4]
                    cs.execute(f"SELECT * FROM {utl.mbots} WHERE status=1 AND ({where}) LIMIT 1")
                    if cs.fetchone() is None:
                        return message.reply_html(text="❌ هیچ اکانت فعالی در این دسته بندی وجود ندارد", reply_to_message_id=message_id)
                    
                    cs.execute(f"UPDATE {utl.orders} SET cats='{row_orders['cats']}' WHERE id={row_orders['id']}")
                    keyboard = [[{'text': utl.menu_var}, {'text': "⏩ بعدی"}]]
                    cs.execute(f"SELECT * FROM {utl.cats}")
                    result = cs.fetchall()
                    for row in result:
                        keyboard.append([{'text': row['name']}])
                    return message.reply_html(
                        text=f"✅ دسته بندی <b>{row_cats['name']}</b> انتخاب شد\n\n"+
                            "روی گزینه <b>⏩ بعدی</b> بزنید یا یک دسته بندی دیگر انتخاب کنید:",
                        reply_markup={'resize_keyboard': True, 'keyboard': keyboard}
                    )
            if ex_step[2] == 'type_send':
                if text == 'خیر':
                    type_send = 0
                elif text == 'بله':
                    type_send = 1
                else:
                    return message.reply_html(text="⛔️ از منو انتخاب کنید", reply_to_message_id=message_id)
                cs.execute(f"UPDATE {utl.orders} SET type_send={type_send} WHERE id={row_orders['id']}")
                cs.execute(f"UPDATE {utl.users} SET step='{ex_step[0]};{ex_step[1]};type' WHERE user_id={from_id}")
                return message.reply_html(
                    text="نوع سفارش را انتخاب کنید:",
                    reply_markup={'resize_keyboard': True,'keyboard': [
                        [{'text': "🔴 لینک گروه 🔴"}],
                        [{'text': "🔵 لیست اعضا 🔵"}],
                        [{'text': utl.menu_var}]
                    ]}
                )
            if ex_step[2] == 'type':
                if text == "🔴 لینک گروه 🔴":
                    cs.execute(f"UPDATE {utl.users} SET step='{ex_step[0]};{ex_step[1]};link;info' WHERE user_id={from_id}")
                    return message.reply_html(
                        text="مطابق نمونه ارسال کنید:\n\n"
                            "لینک گروه (خط اول)\n"
                            "تعداد ارسال (خط دوم)\n\n"
                            "مثال:\n"
                            "https://t.me/group\n"
                            "100",
                        disable_web_page_preview=True,
                        reply_markup={'resize_keyboard': True, 'keyboard': [[{'text': utl.menu_var}]]}
                    )
                if text == "🔵 لیست اعضا 🔵":
                    cs.execute(f"UPDATE {utl.users} SET step='{ex_step[0]};{ex_step[1]};list;info' WHERE user_id={from_id}")
                    return message.reply_html(
                        text="هر کدام از یوزرنیم ها را در یک خط داخل یک فایل txt وارد کنید و فایل را ارسال کنید:",
                        reply_markup={'resize_keyboard': True, 'keyboard': [[{'text': utl.menu_var}]]}
                    )
                return message.reply_html(text="⛔️ از منو انتخاب کنید", reply_to_message_id=message_id)
            if ex_step[2] == 'link':
                if ex_step[3] == 'info':
                    cs.execute(f"SELECT * FROM {utl.mbots} WHERE status>0 ORDER BY RAND()")
                    row_mbots = cs.fetchone()
                    if row_mbots is None:
                        return message.reply_html(text="❌ هیچ اکانتی یافت نشد", reply_to_message_id=message_id)
                    try:
                        ex_nl_text = text.split("\n")
                        group_link = ex_nl_text[0].replace("/+","/joinchat/")
                        count = int(ex_nl_text[1])
                        ex_nl_text = text.split("\n")
                        if len(group_link) > 200 or len(ex_nl_text) != 2:
                            return message.reply_html(text="❌ ورودی نامعتبر", reply_to_message_id=message_id)
                        if group_link[0:13] != "https://t.me/":
                            return message.reply_html(text="❌ لینک گروه اشتباه است", reply_to_message_id=message_id)
                        
                        cs.execute(f"UPDATE {utl.orders} SET group_link='{group_link}',count={count} WHERE id={row_orders['id']}")
                        info_msg = message.reply_html(text="در حال اتصال ...", reply_to_message_id=message_id)
                        subprocess.Popen([utl.python_version, f"{directory}/tl_analyze.py", row_mbots['uniq_id'], str(from_id), str(row_orders['id']), 'analyze', str(info_msg.message_id)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
                        return info_msg.delete()
                    except:
                        return message.reply_html(text="❌ خطای ناشناخته، مجدد تلاش کنید", reply_to_message_id=message_id)
                if ex_step[3] == 'type_users':
                    if text == "همه کاربران":
                        type_users = 0
                    elif text == "کاربران واقعی":
                        type_users = 1
                        cs.execute(f"DELETE FROM {utl.analyze} WHERE order_id={row_orders['id']} AND is_real=0")
                    elif text == "کاربران فیک":
                        type_users = 2
                        cs.execute(f"DELETE FROM {utl.analyze} WHERE order_id={row_orders['id']} AND is_fake=0")
                    elif text == "کاربران آنلاین":
                        type_users = 3
                        cs.execute(f"DELETE FROM {utl.analyze} WHERE order_id={row_orders['id']} AND is_online=0")
                    elif text == "کاربران با شماره":
                        type_users = 4
                        cs.execute(f"DELETE FROM {utl.analyze} WHERE order_id={row_orders['id']} AND is_phone=0")
                    else:
                        return message.reply_html(text="⛔️ فقط از منو انتخاب کنید", reply_to_message_id=message_id)
                    
                    cs.execute(f"SELECT COUNT(*) as count FROM {utl.analyze}")
                    max_users = cs.fetchone()['count']
                    cs.execute(f"UPDATE {utl.orders} SET max_users={max_users},type_users={type_users},send_per_h={row_admin['send_per_h']},created_at={timestamp},updated_at={timestamp} WHERE id={row_orders['id']}")
                    cs.execute(f"UPDATE {utl.users} SET step='{ex_step[0]};{ex_step[1]};get_message;1;1' WHERE user_id={from_id}")
                    return message.reply_html(
                        text="پیامی که میخواهید به کاربران بفرستید را ارسال کنید:",
                        reply_to_message_id=message_id,
                        reply_markup={'resize_keyboard': True,'keyboard': [[{'text': utl.menu_var}]]}
                    )
            if ex_step[2] == 'list':
                if ex_step[3] == 'info':
                    if not message.document:
                        return message.reply_html(text="❌ فقط یک فایل txt ارسال کنید", reply_to_message_id=message_id)
                    
                    info_msg = message.reply_html(text="در حال بررسی ...", reply_to_message_id=message_id)
                    try:
                        list_members = []
                        info_action = bot.get_file(message.document.file_id)
                        with open(f"{directory}/files/id-{row_orders['id']}.txt", "wb") as file:
                            file.write(requests.get(info_action.file_path).content)
                        with open(f"{directory}/files/id-{row_orders['id']}.txt", "rb") as file:
                            result = file.read().splitlines()
                            for value in result:
                                value = value.decode('utf8')
                                if value == "" or len(value) < 5:
                                    continue
                                elif value[0:1] != "@":
                                    value = f"@{value}"
                                if not value in list_members:
                                    list_members.append(value)
                        cs.execute(f"DELETE FROM {utl.analyze}")
                        for i, value in enumerate(list_members):
                            # group users into batches of 3 (batch numbering starts at 1)
                            batch = int((i // 3) + 1)
                            cs.execute(
                                f"INSERT INTO {utl.analyze} (order_id,user_id,username,is_real,created_at,batch) "
                                f"VALUES ({row_orders['id']},0,'{value}',1,{timestamp},{batch})"
                            )
                        if row_orders['type_send'] == 1:
                            i = 0
                            timestamp_start = timestamp = int(time.time())
                            cs.execute(f"SELECT {utl.analyze}.id as id,{utl.analyze}.username as username FROM {utl.analyze} INNER JOIN {utl.reports} ON {utl.analyze}.username={utl.reports}.username GROUP BY {utl.reports}.username")
                            count = cs.rowcount
                            result_detect_members = cs.fetchall()
                            for row in result_detect_members:
                                try:
                                    cs.execute(f"DELETE FROM {utl.analyze} WHERE username='{row['username']}'")
                                    if (int(time.time()) - timestamp_start) > 5:
                                        timestamp_start = int(time.time())
                                        info_msg.edit_text(
                                            text="⏳ در حال جدا سازی کاربران...\n\n"+
                                                f"🔗 لینک: {row_orders['group_link']}\n"+
                                                f"♻️ در حال پیشرفت: {(i / count * 100):.2f}%\n"+
                                                "➖➖➖➖➖➖\n"+
                                                f"📅 مدت زمان: {jdatetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=3, minutes=30))).strftime('%H:%M:%S')}\n"+
                                                f"📅 زمان حال: {utl.convert_time((timestamp_start - timestamp), 2)}",
                                            disable_web_page_preview=True,
                                        )
                                except:
                                    pass
                                i += 1

                        cs.execute(f"SELECT COUNT(*) as count FROM {utl.analyze}")
                        max_users = cs.fetchone()['count']
                        cs.execute(f"UPDATE {utl.orders} SET max_users={max_users},count={max_users},type_users=0,send_per_h={row_admin['send_per_h']},created_at={timestamp},updated_at={timestamp} WHERE id={row_orders['id']}")
                        # start composing messages for batch 1, message index 1
                        cs.execute(f"UPDATE {utl.users} SET step='{ex_step[0]};{ex_step[1]};get_message;1;1' WHERE user_id={from_id}")
                        message.reply_html(text="پیامی که میخواهید به کاربران بفرستید را ارسال کنید:", reply_to_message_id=message_id)
                    except:
                        message.reply_html(text="❌ هنگام آنالیز فایل خطایی رخ داد", reply_to_message_id=message_id)
                    return info_msg.delete()
            if ex_step[2] == "get_message":
                # parse batch and msg_index from step (defaults to 1,1)
                batch = 1
                msg_index = 1
                try:
                    if len(ex_step) >= 5:
                        batch = int(ex_step[3])
                        msg_index = int(ex_step[4])
                except:
                    batch = 1
                    msg_index = 1

                # Global finish: use current composed templates and run all targets with batch=1
                if text == "✅ پایان کلی ✅":
                    try:
                        # collapse all analyze rows for this order into batch 1
                        cs.execute(f"UPDATE {utl.analyze} SET batch=1 WHERE order_id={row_orders['id']}")
                    except Exception:
                        pass
                    cs.execute(f"UPDATE {utl.orders} SET status=1 WHERE id={row_orders['id']}")
                    cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                    return user_panel(message=message, text=f"✅ سفارش با پایان کلی ثبت شد: /order_{row_orders['id']}")

                if text != "✅ پایان ✅":
                    if not message.text and not message.photo and message.video and message.audio and message.voice and message.document:
                        return message.reply_html(text="⛔️ پیام پشتیبانی نمی شود", reply_to_message_id=message_id)
                    try:
                        uniq_id = utl.unique_id()
                        if message.text:
                            info_msg = bot.send_message(chat_id=f"@{row_admin['cache']}", disable_web_page_preview=True, text=txtcap, parse_mode='HTML')
                            type_message = "message"
                        elif message.photo:
                            info_msg = bot.send_photo(chat_id=f"@{row_admin['cache']}", photo=message.photo[len(message.photo) - 1].file_id, caption=txtcap, parse_mode='HTML', )
                            type_message = "photo"
                        elif message.video:
                            info_msg = bot.send_video(chat_id=f"@{row_admin['cache']}", video=message.video.file_id, caption=txtcap, parse_mode='HTML', )
                            type_message = "video"
                        elif message.audio:
                            info_msg = bot.send_audio(chat_id=f"@{row_admin['cache']}", audio=message.audio.file_id, parse_mode='HTML', caption=txtcap, )
                            type_message = "audio"
                        elif message.voice:
                            info_msg = bot.send_voice(chat_id=f"@{row_admin['cache']}", voice=message.voice.file_id, caption=txtcap, parse_mode='HTML', )
                            type_message = "voice"
                        elif message.document:
                            info_msg = bot.send_document(chat_id=f"@{row_admin['cache']}", document=message.document.file_id, caption=txtcap, parse_mode='HTML')
                            type_message = "document"
                        else:
                            message.reply_html(text="⛔️ پیام پشتیبانی نمی شود", reply_to_message_id=message_id)
                    except:
                        message.reply_html(text="❌ خطایی در ارتباط با کانال کش رخ داد، کانال را مجدد ثبت کنید و همه دسترسی های ادمین را به ربات بدهید", reply_to_message_id=message_id)
                    # insert the file/message with batch and msg_index
                    cs.execute(
                        f"INSERT INTO {utl.files} (order_id,type_message,message_id,created_at,uniq_id,batch,msg_index) "
                        f"VALUES ({row_orders['id']},'{type_message}',{info_msg.message_id},{timestamp},'{uniq_id}',{batch},{msg_index})"
                    )
                    cs.execute(f"SELECT * FROM {utl.files} WHERE uniq_id='{uniq_id}'")
                    row_files = cs.fetchone()
                    if row_files is None:
                        return message.reply_html(text="❌ خطایی رخ داد، مجدد تلاش کنید", reply_to_message_id=message_id)

                # count messages for the current batch only
                cs.execute(f"SELECT COUNT(*) as count FROM {utl.files} WHERE order_id={row_orders['id']} AND batch={batch}")
                count = cs.fetchone()['count']

                if count < 3 and text != "✅ پایان ✅":
                    next_msg_index = count + 1
                    cs.execute(f"UPDATE {utl.users} SET step='{ex_step[0]};{ex_step[1]};get_message;{batch};{next_msg_index}' WHERE user_id={from_id}")
                    return message.reply_html(
                        text=f"ارسال پیام شماره {next_msg_index} برای بتچ {batch}:\n\n"
                            "❕ حداکثر 3 پیام می توانید ارسال کنید",
                        reply_markup={'resize_keyboard': True,'keyboard': [
                            [{'text': "✅ پایان ✅"}, {'text': "✅ پایان کلی ✅"}],
                            [{'text': utl.menu_var}]
                        ]}
                    )

                # move to next batch
                next_batch = batch + 1
                cs.execute(f"SELECT MAX(batch) as max_batch FROM {utl.analyze} WHERE order_id={row_orders['id']}")
                max_batch = cs.fetchone()['max_batch']
                max_batch = int(max_batch) if max_batch is not None else 1

                if next_batch <= max_batch:
                    cs.execute(f"UPDATE {utl.users} SET step='{ex_step[0]};{ex_step[1]};get_message;{next_batch};1' WHERE user_id={from_id}")
                    return message.reply_html(
                        text=f"✅ پیام های بتچ {batch} ثبت شد\n\n"
                            f"حالا پیام شماره 1 برای بتچ {next_batch} را ارسال کنید:",
                        reply_markup={'resize_keyboard': True,'keyboard': [
                            [{'text': "✅ پایان ✅"}, {'text': "✅ پایان کلی ✅"}],
                            [{'text': utl.menu_var}]
                        ]}
                    )

                # no more batches: finalize order
                cs.execute(f"UPDATE {utl.orders} SET status=1 WHERE id={row_orders['id']}")
                cs.execute(f"UPDATE {utl.users} SET step='start' WHERE user_id={from_id}")
                return user_panel(message=message, text=f"✅ سفارش ثبت شد: /order_{row_orders['id']}")
        if text == "➕ ایجاد سفارش":
            cs.execute(f"DELETE FROM {utl.orders} WHERE user_id={from_id} AND status=0")
            cs.execute(f"SELECT * FROM {utl.mbots} WHERE status=1 ORDER BY last_order_at ASC LIMIT 1")
            if cs.fetchone() is None:
                return message.reply_html(text="❌ برای ثبت سفارش باید حداقل یک اکانت فعال داشته باشید", reply_to_message_id=message_id)
            
            uniq_id = utl.unique_id()
            cs.execute(f"INSERT INTO {utl.orders} (user_id,status,status_analyze,created_at,updated_at,uniq_id) VALUES ({from_id},0,0,{timestamp},{timestamp},'{uniq_id}')")
            cs.execute(f"SELECT * FROM {utl.orders} WHERE uniq_id='{uniq_id}'")
            row_orders = cs.fetchone()
            if row_orders is None:
                return message.reply_html(text="❌ خطای ناشناخته، مجدد تلاش کنید", reply_to_message_id=message_id)
            
            cs.execute(f"UPDATE {utl.users} SET step='create_order;{row_orders['id']};category' WHERE user_id={from_id}")
            keyboard = [[{'text': utl.menu_var}, {'text': "⏩ بعدی"}]]
            cs.execute(f"SELECT * FROM {utl.cats}")
            result = cs.fetchall()
            for row in result:
                keyboard.append([{'text': row['name']}])
            return message.reply_html(
                text="یک دسته بندی را انتخاب کنید:",
                reply_markup={'resize_keyboard': True, 'keyboard': keyboard}
            )
        if text == "📋 سفارش ها":
            cs.execute(f"SELECT * FROM {utl.orders} WHERE status>0 ORDER BY id DESC LIMIT 0,{utl.step_page}")
            result = cs.fetchall()
            if not result:
                return message.reply_html(text="❌ لیست خالی است", reply_to_message_id=message_id)
            
            now = jdatetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=3, minutes=30)))
            time_today = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
            time_yesterday = time_today - 86400
            cs.execute(f"SELECT COUNT(*) as count FROM {utl.orders}")
            count = cs.fetchone()['count']
            cs.execute(f"SELECT COUNT(*) as count FROM {utl.orders} WHERE created_at>={time_today}")
            orders_count_today = cs.fetchone()['count']
            cs.execute(f"SELECT COUNT(*) as count FROM {utl.orders} WHERE created_at<{time_today} AND created_at>={time_yesterday}")
            orders_count_yesterday = cs.fetchone()['count']

            cs.execute(f"SELECT sum(count_done) FROM {utl.orders} WHERE status=2")
            orders_count_moved_all = cs.fetchone()['sum(count_done)']
            orders_count_moved_all = orders_count_moved_all if orders_count_moved_all is not None else 0
            cs.execute(f"SELECT sum(count_done) FROM {utl.orders} WHERE status=2 AND created_at>={time_today}")
            orders_count_moved_today = cs.fetchone()['sum(count_done)']
            orders_count_moved_today = orders_count_moved_today if orders_count_moved_today is not None else 0
            cs.execute(f"SELECT sum(count_done) FROM {utl.orders} WHERE status=2 AND created_at<{time_today} AND created_at>={time_yesterday}")
            orders_count_moved_yesterday = cs.fetchone()['sum(count_done)']
            orders_count_moved_yesterday = orders_count_moved_yesterday if orders_count_moved_yesterday is not None else 0

            output = f"📋 کل سفارش ها: {count} ({orders_count_moved_all})\n"
            output += f"🟢 سفارش های امروز: {orders_count_today} ({orders_count_moved_today})\n"
            output += f"⚪️ سفارش های دیروز: {orders_count_yesterday} ({orders_count_moved_yesterday})\n\n"
            i = 1
            for row in result:
                group_link = f"<a href='{row['group_link']}'>{row['group_link'].replace('https://t.me/', '')}</a>" if row['group_link'] is not None else "با فایل انجام شده"
                output += f"{i}. جزییات: /order_{row['id']}\n"
                output += f"🔹️ گروه: {group_link}\n"
                output += f"🔹️ انجام شده / درخواستی: [{row['count_done']} / {row['count']}]\n"
                output += f"🔹️ وضعیت: {utl.status_orders[row['status']]}\n"
                output += f"📅️ ایجاد: {jdatetime.datetime.fromtimestamp(row['created_at']).astimezone(datetime.timezone(datetime.timedelta(hours=3, minutes=30))).strftime('%Y/%m/%d %H:%M')}\n\n"
                i += 1
            ob = utl.Pagination(update, "orders", output, utl.step_page, count)
            return ob.process()
        if text == "➕ افزودن اکانت":
            cs.execute(f"DELETE FROM {utl.mbots} WHERE creator_user_id={from_id} AND status=0 AND user_id IS NULL")
            row_apis = utl.select_api(cs, row_admin['api_per_number'])
            if row_apis is None:
                return message.reply_html(text="❌ ابتدا یک API اضافه کنید یا از تنظیمات گزینه اول را افزایش دهید", reply_to_message_id=message_id)
            
            uniq_id = utl.unique_id()
            cs.execute(f"INSERT INTO {utl.mbots} (cat_id,creator_user_id,api_id,api_hash,status,created_at,uniq_id) VALUES (1,{from_id},{row_apis['api_id']},'{row_apis['api_hash']}',0,{timestamp},'{uniq_id}')")
            cs.execute(f"SELECT * FROM {utl.mbots} WHERE uniq_id='{uniq_id}'")
            row_mbots = cs.fetchone()
            if row_mbots is None:
                return message.reply_html(text="❌ خطایی رخ داد، مجدد تلاش کنید")
            
            cs.execute(f"UPDATE {utl.users} SET step='add_acc;{row_mbots['id']};type' WHERE user_id={from_id}")
            return message.reply_html(
                text="روش افزودن اکانت را انتخاب کنید:",
                reply_markup={'resize_keyboard': True,'keyboard': [
                    [{'text': 'زیپ'}, {'text': 'سشن'}, {'text': 'شماره'}],
                    [{'text': utl.menu_var}]
                ]}
            )
        if text == "📋 اکانت ها":
            cs.execute(f"SELECT COUNT(*) as count FROM {utl.mbots} WHERE user_id IS NOT NULL")
            accs_all = cs.fetchone()['count']
            cs.execute(f"SELECT COUNT(*) as count FROM {utl.mbots} WHERE user_id IS NOT NULL AND status=0")
            accs_logout = cs.fetchone()['count']
            cs.execute(f"SELECT COUNT(*) as count FROM {utl.mbots} WHERE status=1")
            accs_active = cs.fetchone()['count']
            cs.execute(f"SELECT COUNT(*) as count FROM {utl.mbots} WHERE status=2")
            accs_restrict = cs.fetchone()['count']
            return message.reply_html(
                text="📋 اکانت ها\n\n"
                    "❌ محدود شده: اکانت ها بعد از «محدود شدن توسط تلگرام» یا «گزینه سروم تنظیمات» در این وضعیت قرار میگیرند و بعد از تمام محدودیت خودکار از این حالت خارج می شوند\n\n"
                    "⛔️ لاگ اوت شده: اکانت هایی که لاگ اوت یا توسط تلگرام بن شده اند\n\n"
                    "✅ فعال: اکانت هایی که در ربات لاگین و قابل استفاده هستند",
                reply_markup={'inline_keyboard': [
                    [{'text': f"💢 همه ({accs_all}) 💢", 'callback_data': f"pg;accounts;1"}],
                    [
                        {'text': f"⛔️ لاگ اوت شده ({accs_logout})", 'callback_data': f"pg;0;1"},
                        {'text': f"❌ محدود شده ({accs_restrict})", 'callback_data': f"pg;2;1"}
                    ],
                    [{'text': f"✅ فعال ({accs_active})", 'callback_data': f"pg;1;1"}],
                    [{'text': "👇 دستورات عمومی 👇", 'callback_data': "nazan"}],
                    [{'text': "✔️ حذف لاگ اوت شده ها ✔️", 'callback_data': "gc;1"}],
                ]}
            )
        if text == "➕ افزودن API":
            cs.execute(f"UPDATE {utl.users} SET step='add_api;' WHERE user_id={from_id}")
            return message.reply_html(
                text="‏ API را مطابق نمونه ارسال کنید:\n\n"
                    "مثال:\n"
                    "‏api id (در خط اول)\n"
                    "‏api hash (در خط دوم)",
                reply_markup={'resize_keyboard': True, 'keyboard': [[{'text': utl.menu_var}]]}
            )
        if text == "‏📋 API ها":
            cs.execute(f"SELECT * FROM {utl.apis} ORDER BY id DESC LIMIT 0,{utl.step_page}")
            result = cs.fetchall()
            if not result:
                return message.reply_html(text="❌ لیست API خالی است", reply_to_message_id=message_id)
            
            cs.execute(f"SELECT COUNT(*) as count FROM {utl.apis}")
            rowcount = cs.fetchone()['count']
            output = f"‏📜 API ها ({rowcount})\n\n"
            for row in result:
                output += f"‏🔴️ Api ID: ‏<code>{row['api_id']}</code>\n"
                output += f"‏🔴️ Api Hash: ‏<code>{row['api_hash']}</code>\n"
                output += f"❌ حذف: /DeleteApi_{row['id']}\n\n"
            ob = utl.Pagination(update, "apis", output, utl.step_page, rowcount)
            return ob.process()
        if text == "➕ ایجاد دسته بندی":
            cs.execute(f"UPDATE {utl.users} SET step='create_cat;none' WHERE user_id={from_id}")
            return message.reply_html(
                text="نام دسته بندی را وارد کنید:",
                reply_markup={'resize_keyboard': True,'keyboard': [[{'text': utl.menu_var}]]}
            )
        if text == "📋 دسته بندی ها":
            cs.execute(f"SELECT * FROM {utl.cats} ORDER BY id DESC LIMIT 0,{utl.step_page}")
            result = cs.fetchall()
            if not result:
                return message.reply_html(text="❌ لیست خالی است", reply_to_message_id=message_id)
            
            cs.execute(f"SELECT COUNT(*) as count FROM {utl.cats}")
            rowcount = cs.fetchone()['count']
            output = f"📋 دسته بندی ها ({rowcount})\n\n"
            i = 1
            for row in result:
                cs.execute(f"SELECT COUNT(*) as count FROM {utl.mbots} WHERE cat_id={row['id']}")
                count_mbots = cs.fetchone()['count']
                output += f"{i}. ‏{row['name']} ‏({count_mbots} اکانت)\n"
                output += f"❌ حذف: /DeleteCat_{row['id']}\n\n"
                i += 1
            ob = utl.Pagination(update, "categories", output, utl.step_page, rowcount)
            return ob.process()
        if text == "🔮 آنالیز":
            cs.execute(f"SELECT * FROM {utl.mbots} WHERE status>0")
            row_mbots = cs.fetchone()
            if row_mbots is None:
                return message.reply_html(text="❌ هیچ اکانتی یافت نشد", reply_to_message_id=message_id)
            
            cs.execute(f"UPDATE {utl.users} SET step='analyze;type' WHERE user_id={from_id}")
            return message.reply_html(
                text="نوع آنالیز را انتخاب کنید:",
                reply_markup={'resize_keyboard': True,'keyboard': [
                    [{'text': 'پیام ها'}, {'text': 'کاربران'}],
                    [{'text': utl.menu_var}],
                ]}
            )
        if text == "📩 پیام‌ها":
            # show grouped inbox by registered accounts (mbots)
            cs.execute(f"SELECT mbot_id, COUNT(*) as cnt FROM {utl.inbox} WHERE processed=0 GROUP BY mbot_id ORDER BY cnt DESC")
            groups = cs.fetchall()
            if not groups:
                return message.reply_text(text="❌ صندوق پیام خالی است", reply_to_message_id=message_id)
            output = "📩 صندوق پیام‌ها (گروه‌بندی بر اساس اکانت‌ها):\n\n"
            i = 1
            for g in groups:
                cs.execute(f"SELECT * FROM {utl.mbots} WHERE id=%s", (g['mbot_id'],))
                mb = cs.fetchone()
                mb_phone = mb['phone'] if mb is not None else 'unknown'
                output += f"{i}. /inboxm_{g['mbot_id']} — اکانت: <code>{mb_phone}</code> — {g['cnt']} پیام\n"
                i += 1
            output += "\nبرای دیدن همه پیام‌ها از همه اکانت‌ها: /inbox_all"
            return message.reply_text(text=output, parse_mode='HTML', disable_web_page_preview=True)
        if text.startswith('/inboxm_'):
            # list messages for a specific mbot: /inboxm_<mbot_id>
            try:
                mbot_id = int(text.split('_', 1)[1])
            except:
                return message.reply_text(text="❌ شناسه اکانت نامعتبر", reply_to_message_id=message_id)
            # show threads (senders) for this mbot, grouped by thread_id
            # use aggregate functions for non-grouped columns to satisfy ONLY_FULL_GROUP_BY
            cs.execute(f"SELECT thread_id, MAX(from_id) as from_id, MAX(from_username) as from_username, MAX(from_first_name) as from_first_name, MAX(from_last_name) as from_last_name, COUNT(*) as cnt, MAX(created_at) as last_created FROM {utl.inbox} WHERE processed=0 AND mbot_id=%s GROUP BY thread_id ORDER BY last_created DESC LIMIT %s", (mbot_id, utl.step_page))
            threads = cs.fetchall()
            if not threads:
                return message.reply_text(text="❌ پیام برای این اکانت وجود ندارد", reply_to_message_id=message_id)
            output = f"📩 گفتگوها برای اکانت {mbot_id}:\n\n"
            i = 1
            for th in threads:
                sender = th['from_username'] if th['from_username'] is not None else (str(th['from_id']) if th['from_id'] is not None else 'ناشناس')
                tid = th['thread_id']
                # thread command: /inboxthread_<mbot_id>_<thread_id>
                output += f"{i}. /inboxthread_{mbot_id}_{tid} — از: {sender} — {th['cnt']} پیام\n"
                i += 1
            output += "\nبرای باز کردن هر گفتگو، دستور بالا را ارسال کنید"
            return message.reply_text(text=output, parse_mode='HTML', disable_web_page_preview=True)
        if text.startswith('/inboxthread_'):
            # view messages in a specific thread: /inboxthread_<mbot_id>_<thread_id>
            parts = text.split('_', 2)
            if len(parts) < 3:
                return message.reply_text(text="❌ دستور نامعتبر", reply_to_message_id=message_id)
            try:
                mbot_id = int(parts[1])
            except:
                return message.reply_text(text="❌ شناسه اکانت نامعتبر", reply_to_message_id=message_id)
            thread_id = parts[2]
            cs.execute(f"SELECT * FROM {utl.inbox} WHERE processed=0 AND mbot_id=%s AND thread_id=%s ORDER BY created_at DESC LIMIT 0,%s", (mbot_id, thread_id, utl.step_page))
            msgs = cs.fetchall()
            if not msgs:
                return message.reply_text(text="❌ هیچ پیامی در این گفتگو وجود ندارد", reply_to_message_id=message_id)
            output = f"📨 پیام‌های گفتگو ({len(msgs)})\n\n"
            i = 1
            for m in msgs:
                sender = m['from_username'] if m['from_username'] is not None else (str(m['from_id']) if m['from_id'] is not None else 'ناشناس')
                output += f"{i}. /inbox_{m['id']} — از: {sender} — {m['text'][:100]}\n"
                i += 1
            output += "\nبرای علامت‌گذاری همه پیام‌های این گفتگو به عنوان خوانده شده، از دکمه زیر استفاده کنید"
            # reply with an inline button to mark the entire thread as read
            # ensure inline keyboard buttons include callback_data (text-only buttons are invalid in inline_keyboard)
            return message.reply_text(text=output, parse_mode='HTML', disable_web_page_preview=True, reply_markup={'inline_keyboard': [[{'text': 'علامت خوانده شده (گفتگو) ✅', 'callback_data': f"markreadthread;{mbot_id};{thread_id}"}], [{'text': utl.menu_var, 'callback_data': 'menu'}]]})
        if text == '/inbox_all':
            cs.execute(f"SELECT * FROM {utl.inbox} WHERE processed=0 ORDER BY created_at DESC LIMIT 0,{utl.step_page}")
            result = cs.fetchall()
            if not result:
                return message.reply_text(text="❌ صندوق پیام خالی است", reply_to_message_id=message_id)
            output = "📩 صندوق پیام‌ها:\n\n"
            i = 1
            for row in result:
                cs.execute(f"SELECT * FROM {utl.mbots} WHERE id=%s", (row['mbot_id'],))
                mb = cs.fetchone()
                mb_phone = mb['phone'] if mb is not None else 'unknown'
                sender = row['from_username'] if row['from_username'] is not None else str(row['from_id'])
                output += f"{i}. /inbox_{row['id']} — از: {sender} — اکانت: <code>{mb_phone}</code>\n"
                i += 1
            return message.reply_text(text=output, parse_mode='HTML', disable_web_page_preview=True)
        if text == "⚙️ تنظیمات":
            return message.reply_html(
                text="⚙️ تنظیمات",
                reply_markup={'inline_keyboard': [
                    [{'text': f"📝 در هر API چند اکانت ثبت شود: {row_admin['api_per_number']} اکانت",'callback_data': "settings;api_per_number"}],
                    [{'text': f"📝 ارسال هر اکانت در هر استفاده: {row_admin['send_per_h']} ارسال",'callback_data': "settings;send_per_h"}],
                    [{'text': (f"📝 استفاده اکانت هر چند ساعت: " + (f"{int(row_admin['limit_per_h'] / 3600)} ساعت" if row_admin['limit_per_h'] > 0 else "غیرفعال ❌")),'callback_data': "settings;limit_per_h"}],
                    [{'text': f"🔐 رمز دو مرحله ای: " + (row_admin['account_password'] if row_admin['account_password'] is not None else "ثبت نشده") + "",'callback_data': "settings;account_password"}],
                    [{'text': ("تنظیم / تغییر رمز دو مرحله ای: " + ("فعال ✅" if row_admin['change_pass'] > 0 else "غیرفعال ❌")),'callback_data': "settings;change_pass"}],
                    [{'text': ("خروج از بقیه سشن ها: " + ("فعال ✅" if row_admin['exit_session'] > 0 else "غیرفعال ❌")),'callback_data': "settings;exit_session"}],
                    [{'text': ("تنظیم نام، بیو و پروفایل: " + ("فعال ✅" if row_admin['is_change_profile'] > 0 else "غیرفعال ❌")),'callback_data': "settings;is_change_profile"}],
                    [{'text': ("تنظیم یوزرنیم: " + ("فعال ✅" if row_admin['is_set_username'] > 0 else "غیرفعال ❌")),'callback_data': "settings;is_set_username"}],
                    [{'text': ("شنود پیام‌ها: " + ("غیرفعال ❌" if row_admin.get('disable_inbox', 0) > 0 else "فعال ✅")), 'callback_data': "settings;inbox_listen"}],
                ]}
            )
        if text == "👤 کاربر":
            cs.execute(f"UPDATE {utl.users} SET step='info_user;' WHERE user_id={from_id}")
            return message.reply_html(
                text="آیدی عددی کاربر را ارسال کنید:\n\n"
                    "❕ برای بدست آوردن آیدی عددی می توانید از ربات @info_tel_bot استفاده کنید",
                reply_markup={'resize_keyboard': True,'keyboard': [[{'text': utl.menu_var}]]}
            )
        if ex_text[0] == '/order':
            cs.execute(f"SELECT * FROM {utl.orders} WHERE id={int(ex_text[1])}")
            row_orders = cs.fetchone()
            if row_orders is None:
                return message.reply_html(text="❌ سفارش یافت نشد")
            
            if row_orders['group_link'] is not None:
                output = f"\n🆔 <code>{row_orders['group_id']}</code>\n"
                output += f"🔗 {row_orders['group_link']}\n\n"
            else:
                output = "از طریق لیست انجام شده\n\n"
            if row_orders['cats'] is None:
                cats = "پشتیبانی نمی شود"
            else:
                where = ""
                cats = row_orders['cats'].split(",")
                for category in cats:
                    where += f"id={int(category)} OR "
                where = where[0:-4]
                cats = ""
                cs.execute(f"SELECT * FROM {utl.cats} WHERE {where}")
                result = cs.fetchall()
                for row in result:
                    cats += f"{row['name']},"
                cats = cats[0:-1]
            return message.reply_html(
                text=f"اطلاعات گروه: {output}"
                    f"👤 ارسال شده / درخواستی: [{row_orders['count_done']:,} / {row_orders['count']:,}]\n"
                    f"👤 در حال بررسی / همه: [{row_orders['count_request']:,} / {row_orders['max_users']:,}]\n\n"
                    f"🔵 گزارش اکانت ها\n"
                    f"      استفاده شده: {row_orders['count_acc']:,}\n"
                    f"      محدود شده: {row_orders['count_restrict']:,}\n"
                    f"      ریپورت شده: {row_orders['count_report']:,}\n"
                    f"      از دست رفته: {row_orders['count_accout']:,}\n\n"
                    f"🔴 گزارش درخواست های ارسال\n"
                    f"      خطا های اسپم: {row_orders['count_usrspam']:,}\n"
                    f"      یوزرنیم اشتباه: {row_orders['count_userincorrect']:,}\n"
                    f"      اکانت های محدود: {row_orders['count_restrict_error']:,}\n"
                    f"      خطا های دیگر: {row_orders['count_other_errors']:,}\n\n"
                    f"🟣 دسته بندی ها: {cats}\n"
                    f"🟣 تعداد ارسال هر اکانت: {row_orders['send_per_h']:,}\n\n"
                    f"📥 خروجی کاربران باقی مانده: /exo_{row_orders['id']}_r\n"
                    f"📥 خروجی کاربران منتقل شده: /exo_{row_orders['id']}_m\n"
                    "➖➖➖➖➖➖\n"
                    f"📅️ ایجاد: {jdatetime.datetime.fromtimestamp(row_orders['created_at']).astimezone(datetime.timezone(datetime.timedelta(hours=3, minutes=30))).strftime('%Y/%m/%d %H:%M:%S')}\n"
                    f"📅️ بروزرسانی: {jdatetime.datetime.fromtimestamp(row_orders['updated_at']).astimezone(datetime.timezone(datetime.timedelta(hours=3, minutes=30))).strftime('%Y/%m/%d %H:%M:%S')}\n"
                    f"📅 الان: {jdatetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=3, minutes=30))).strftime('%Y/%m/%d %H:%M:%S')}",
                reply_markup={'inline_keyboard': [
                    [{'text': utl.status_orders[row_orders['status']], 'callback_data': (f"change_status;{row_orders['id']};2" if row_orders['status'] == 1 else "nazan")}],
                    [{'text': '🔄 بروزرسانی 🔄', 'callback_data': f"update;{row_orders['id']}"}]
                ]}
            )
        if ex_text[0] == '/inbox':
            try:
                inbox_id = int(ex_text[1])
            except:
                return message.reply_html(text="❌ شناسه نامعتبر")
            cs.execute(f"SELECT * FROM {utl.inbox} WHERE id={inbox_id}")
            row_in = cs.fetchone()
            if row_in is None:
                return message.reply_html(text="❌ پیام یافت نشد")
            cs.execute(f"SELECT * FROM {utl.mbots} WHERE id={row_in['mbot_id']}")
            mb = cs.fetchone()
            mb_phone = mb['phone'] if mb is not None else 'unknown'
            sender = row_in['from_username'] if row_in['from_username'] is not None else str(row_in['from_id'])
            # use parameterized query to avoid f-string parsing issues and SQL injection
            step_value = f"reply_inbox;{row_in['id']};{row_in['mbot_id']}"
            cs.execute(f"UPDATE {utl.users} SET step=%s WHERE user_id=%s", (step_value, from_id))
            # provide an inline button to mark the message as read
            cb_mark = 'markread;' + str(row_in['id'])
            # the second row must contain inline-button objects; add a simple callback_data for the menu button
            reply_k = {'inline_keyboard': [[{'text': 'علامت خوانده شده ✅', 'callback_data': cb_mark }], [{'text': utl.menu_var, 'callback_data': 'menu'}]]}
            return message.reply_text(
                text=f"📩 پیام از: {sender}\nآیدی: {row_in['from_id']}\nاکانت دریافتی: <code>{mb_phone}</code>\n\nمتن:\n{row_in['text']}\n\nبرای پاسخ، متن را ارسال کنید (ارسال به عنوان پاسخ توسط اکانت انتخاب‌شده).",
                parse_mode='HTML',
                reply_markup=reply_k
            )
        if ex_text[0] == '/category':
            cs.execute(f"SELECT * FROM {utl.mbots} WHERE id={int(ex_text[1])}")
            row_mbots = cs.fetchone()
            if row_mbots is None:
                return message.reply_html(text="❌ اکانت یافت نشد", reply_to_message_id=message_id)
            
            cs.execute(f"UPDATE {utl.users} SET step='set_cat;{row_mbots['id']}' WHERE user_id={from_id}")
            keyboard = []
            cs.execute(f"SELECT * FROM {utl.cats}")
            result = cs.fetchall()
            for row in result:
                keyboard.append([{'text': row['name']}])
            keyboard.append([{'text': utl.menu_var}])
            return message.reply_html(
                text="یکی از دسته بندی ها را انتخاب کنید:",
                reply_markup={'resize_keyboard': True,'keyboard': keyboard}
            )
        if ex_text[0] == '/DeleteCat':
            cs.execute(f"SELECT * FROM {utl.cats} WHERE id={int(ex_text[1])}")
            row_cats = cs.fetchone()
            if row_cats is None:
                return message.reply_html(text="❌ دسته بندی یافت نشد", reply_to_message_id=message_id)
            if row_cats['id'] == 1:
                return message.reply_html(text="❌ دسته بندی قابل حذف نیست")
            
            cs.execute(f"SELECT COUNT(*) as count FROM {utl.mbots} WHERE cat_id={row_cats['id']}")
            count = cs.fetchone()['count']
            if count < 1:
                cs.execute(f"DELETE FROM {utl.cats} WHERE id={row_cats['id']}")
                return message.reply_html(text="✅ با موفقیت حذف شد", reply_to_message_id=message_id)
            
            return message.reply_html(
                text=f"❌ حذف دسته بندی: {row_cats['name']}\n\n"
                    f"/DeleteCatConfirm_{row_cats['id']}\n\n"
                    f"⚠️ {count} اکانت در این دسته بندی ثبت شده است",
                reply_to_message_id=message_id
            )
        if ex_text[0] == '/DeleteCatConfirm':
            cs.execute(f"SELECT * FROM {utl.cats} WHERE id={int(ex_text[1])}")
            row_cats = cs.fetchone()
            if row_cats is None:
                return message.reply_html(text="❌ دسته بندی یافت نشد", reply_to_message_id=message_id)
            if row_cats['id'] == 1:
                return message.reply_html(text="❌ دسته بندی قابل حذف نیست")
            
            cs.execute(f"UPDATE {utl.mbots} SET cat_id=1 WHERE cat_id={row_cats['id']}")
            cs.execute(f"DELETE FROM {utl.cats} WHERE id={row_cats['id']}")
            return message.reply_html(text="✅ با موفقیت حذف شد", reply_to_message_id=message_id)
        if ex_text[0] == '/status':
            cs.execute(f"SELECT * FROM {utl.mbots} WHERE id={int(ex_text[1])}")
            row_mbots = cs.fetchone()
            if row_mbots is None:
                return message.reply_html(text="❌ اکانت یافت نشد", reply_to_message_id=message_id)
            
            info_msg = message.reply_html(text="در حال اتصال ...", reply_to_message_id=message_id)
            # spawn status checker in background so the bot remains responsive
            subprocess.Popen([utl.python_version, f"{directory}/tl_account_status.py", row_mbots['uniq_id'], str(from_id), str(info_msg.message_id)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
            return
        if ex_text[0] == '/delete':
            cs.execute(f"SELECT * FROM {utl.mbots} WHERE id={int(ex_text[1])}")
            row_mbots = cs.fetchone()
            if row_mbots is None:
                return message.reply_html(text="❌ اکانت یافت نشد", reply_to_message_id=message_id)
            
            return message.reply_html(
                text=f"❌ حذف اکانت: <code>{row_mbots['phone']}</code>\n\n"
                    f"/deleteconfirm_{ex_text[1]}",
                reply_to_message_id=message_id
            )
        if ex_text[0] == '/deleteconfirm':
            cs.execute(f"SELECT * FROM {utl.mbots} WHERE id={int(ex_text[1])}")
            row_mbots = cs.fetchone()
            if row_mbots is None:
                return message.reply_html(text="❌ اکانت یافت نشد", reply_to_message_id=message_id)
            
            cs.execute(f"DELETE FROM {utl.mbots} WHERE id={row_mbots['id']}")
            return message.reply_html(text=f"‏✅ اکانت <code>{row_mbots['phone']}</code> با موفقیت حذف شد", reply_to_message_id=message_id)
        if ex_text[0] == '/DeleteApi':
            cs.execute(f"SELECT * FROM {utl.apis} WHERE id={int(ex_text[1])}")
            row_apis = cs.fetchone()
            if row_apis is None:
                return message.reply_html(text="‏❌ API یافت نشد", reply_to_message_id=message_id)
            
            cs.execute(f"DELETE FROM {utl.apis} WHERE id={row_apis['id']}")
            return message.reply_html(text="‏✅ API با موفقیت حذف شد", reply_to_message_id=message_id)
        if ex_text[0] == '/ex':
            cs.execute(f"SELECT * FROM {utl.egroup} WHERE id={int(ex_text[1])}")
            row_egroup = cs.fetchone()
            if row_egroup is None:
                return message.reply_html(text="❌ سفارش یافت نشد", reply_to_message_id=message_id)
            if row_egroup['type'] == 0:
                info_msg = message.reply_html(text="در حال ارسال ...")
                try:
                    if ex_text[2] == 'a':
                        message.reply_document(document=open(f"{directory}/export/{row_egroup['id']}/users_all.txt","rb"), caption="همه کاربران", reply_to_message_id=message_id)
                    elif ex_text[2] == 'u':
                        message.reply_document(document=open(f"{directory}/export/{row_egroup['id']}/users_real.txt","rb"), caption="کاربران واقعی", reply_to_message_id=message_id)
                    elif ex_text[2] == 'f':
                        message.reply_document(document=open(f"{directory}/export/{row_egroup['id']}/users_fake.txt","rb"), caption="کاربران فیک", reply_to_message_id=message_id)
                    elif ex_text[2] == 'n':
                        message.reply_document(document=open(f"{directory}/export/{row_egroup['id']}/users_has_phone.txt","rb"), caption="کاربران با شماره", reply_to_message_id=message_id)
                    elif ex_text[2] == 'o':
                        message.reply_document(document=open(f"{directory}/export/{row_egroup['id']}/users_online.txt","rb"), caption="کربران آنلاین", reply_to_message_id=message_id)
                except:
                    return info_msg.edit_text(text="❌ خطایی در ارسال فایل رخ داد")
                return info_msg.delete()
            else:
                info_msg = message.reply_html(text="در حال ارسال ...")
                try:
                    if ex_text[2] == 'a':
                        message.reply_document(document=open(f"{directory}/export/{row_egroup['id']}/users_all.txt","rb"), caption='کاربارن شناسایی شده', reply_to_message_id=message_id)
                    elif ex_text[2] == 'u':
                        message.reply_document(document=open(f"{directory}/export/{row_egroup['id']}/users_username.txt","rb"), caption="کاربران با یوزرنیم", reply_to_message_id=message_id)
                    elif ex_text[2] == 'b':
                        message.reply_document(document=open(f"{directory}/export/{row_egroup['id']}/users_bots.txt","rb"), caption="ربات ها", reply_to_message_id=message_id)
                except:
                    message.reply_html(text="❌ There was a problem uploading the file")
                return info_msg.delete()
        if ex_text[0] == '/exo':
            cs.execute(f"SELECT * FROM {utl.orders} WHERE id={int(ex_text[1])}")
            row_orders = cs.fetchone()
            if row_orders is None:
                return message.reply_html(text="❌ سفارش یافت نشد", reply_to_message_id=message_id)
            if row_orders['status'] != 2:
                return message.reply_html(text="❌ سفارش هنوز تمام نشده است", reply_to_message_id=message_id)
            
            info_msg = message.reply_html(text="در حال ارسال ...")
            if ex_text[2] == 'm':
                if not os.path.exists(f"{directory}/files/exo_{row_orders['id']}_m.txt"):
                    return message.reply_html(text="❌ هیچ ممبری یافت نشد", reply_to_message_id=message_id)
                message.reply_document(document=open(f"{directory}/files/exo_{row_orders['id']}_m.txt", "rb"), caption="کاربران منتقل شده", reply_to_message_id=message_id)
            elif ex_text[2] == 'r':
                if not os.path.exists(f"{directory}/files/exo_{row_orders['id']}_r.txt"):
                    return message.reply_html(text="❌ هیچ ممبری یافت نشد", reply_to_message_id=message_id)
                message.reply_document(document=open(f"{directory}/files/exo_{row_orders['id']}_r.txt", "rb"), caption="کاربران باقی مانده", reply_to_message_id=message_id)
            return info_msg.delete()
        

if __name__ == '__main__':
    updater = telegram.ext.Updater(utl.token)
    dispatcher = updater.dispatcher

    dispatcher.add_handler(telegram.ext.MessageHandler(telegram.ext.Filters.chat_type.private & telegram.ext.Filters.update.message & telegram.ext.Filters.update, private_process, run_async=True))
    dispatcher.add_handler(telegram.ext.CallbackQueryHandler(callbackquery_process, run_async=True))
    
    updater.start_polling()
    updater.idle()
