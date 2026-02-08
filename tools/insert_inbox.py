import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import utility as utl, time
cs = utl.Database().data()
now=int(time.time())
mbot_id=14
tbl = utl.inbox
sql = "INSERT INTO %s (mbot_id, from_id, from_username, text, thread_id, created_at, processed) VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % tbl
cs.execute(sql, (mbot_id, 123456789, "test_user_for_bot", "این یک پیام تست است — برای بررسی ذخیره شدن پیام‌ها", "test-thread", now, 0))
print('Inserted test inbox row')
