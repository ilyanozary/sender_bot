import os, time, subprocess, utility as utl


directory = os.path.dirname(os.path.abspath(__file__))
filename = str(os.path.basename(__file__))

utl.get_params_pids_by_full_script_name(script_names=[f"{directory}/{filename}"], is_kill_proccess=True)
print(f"ok: {filename}")


while True:
    try:
        timestamp = int(time.time())
        cs = utl.Database()
        cs = cs.data()

        cs.execute(f"SELECT * FROM {utl.admin}")
        row_admin = cs.fetchone()

        cs.execute(f"SELECT * FROM {utl.orders} WHERE status=1")
        row_orders = cs.fetchone()
        if row_orders is not None:
            where = ""
            cats = row_orders['cats'].split(",")
            for category in cats:
                where += f"cat_id={int(category)} OR "
            where = where[0:-4]
            try:
                print(f"cron: active order id={row_orders['id']} cats={row_orders['cats']} where=({where}) send_per_h={row_orders['send_per_h']}")
            except Exception:
                pass
            
            cs.execute(f"SELECT * FROM {utl.mbots} WHERE status=1 AND ({where}) ORDER BY last_order_at ASC")
            result_mbots = cs.fetchall()
            try:
                print(f"cron: found {len(result_mbots) if result_mbots else 0} eligible accounts with status=1")
            except Exception:
                pass
            if result_mbots:
                for row_mbots in result_mbots:
                    # فقط بررسی اجرای همین اسکریپت اکانت‌رانر
                    result_pids = utl.get_params_pids_by_full_script_name(script_names=[f"{directory}/tl_run_account.py"], param1=row_mbots['uniq_id'])
                    if not result_pids:
                        # قبل از شروع ارسال، لیسنر اینباکس همین اکانت را می‌بندیم تا لاک جلسه تلگرام آزاد شود
                        try:
                            utl.get_params_pids_by_full_script_name(script_names=[f"{directory}/tl_inbox_listener.py"], param1=row_mbots['uniq_id'], is_kill_proccess=True)
                            print(f"cron: killed inbox listener for uniq_id={row_mbots['uniq_id']}")
                        except Exception:
                            pass
                        # اگر قبلاً در usedaccs ثبت شده بود اما پروسه‌ای در حال اجرا نیست، مجدداً اجرا می‌کنیم
                        try:
                            print(f"cron: starting tl_run_account uniq_id={row_mbots['uniq_id']} bot_id={row_mbots['id']} for order_id={row_orders['id']}")
                        except Exception:
                            pass
                        os.system(f"{utl.python_version} \"{directory}/tl_run_account.py\" {row_mbots['uniq_id']} {row_orders['id']}")
                        # پس از پایان ارسال، لیسنر اینباکس را دوباره راه‌اندازی می‌کنیم
                        try:
                            subprocess.Popen([utl.python_version, f"{directory}/tl_inbox_listener.py", row_mbots['uniq_id']])
                            print(f"cron: restarted inbox listener for uniq_id={row_mbots['uniq_id']}")
                        except Exception:
                            pass

                        cs.execute(f"SELECT * FROM {utl.orders} WHERE id={row_orders['id']}")
                        row_orders = cs.fetchone()
                        if row_orders['status'] == 2:
                            break
                    else:
                        try:
                            print(f"cron: tl_run_account already running for uniq_id={row_mbots['uniq_id']} (pids={result_pids})")
                        except Exception:
                            pass
            
            cs.execute(f"SELECT * FROM {utl.orders} WHERE id={row_orders['id']}")
            row_orders = cs.fetchone()
            if row_orders['status'] != 2:
                utl.end_order(cs, f"{directory}/files/exo_{row_orders['id']}_r.txt", row_orders)
    except Exception as e:
        print(f"Error in main: {e}")
    time.sleep(10)

