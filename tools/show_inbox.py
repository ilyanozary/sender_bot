import utility as utl
cs = utl.Database().data()
cs.execute("SELECT id,mbot_id,from_id,from_username,text,processed,created_at FROM %s WHERE processed=0 ORDER BY created_at DESC LIMIT 0,%s" % (utl.inbox, utl.step_page))
res = cs.fetchall()
if not res:
    print("❌ صندوق پیام خالی است")
else:
    print("📩 صندوق پیام‌ها:\n")
    for i,row in enumerate(res,1):
        sender = row['from_username'] if row['from_username'] else row['from_id']
        text = row['text'] if row['text'] else ''
        print(f"{i}. /inbox_{row['id']} — از: {sender} — اکانت id={row['mbot_id']} text={text[:40]!r}")
