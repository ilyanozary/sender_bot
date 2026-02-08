#!/bin/bash
set -e

echo "=== Sender Bot Container Starting ==="

# Wait for MySQL to be ready
echo "Waiting for MySQL at ${PV_DB_HOST:-localhost}:${PV_DB_PORT:-3306}..."
MAX_RETRIES=30
RETRY_COUNT=0
until python3 -c "
import os, pymysql
try:
    conn = pymysql.connect(
        host=os.getenv('PV_DB_HOST', 'localhost'),
        port=int(os.getenv('PV_DB_PORT', '3306')),
        user=os.getenv('PV_DB_USER', 'becherostam'),
        password=os.getenv('PV_DB_PASS', '123Q'),
        database=os.getenv('PV_DB_NAME', 'pv_bot')
    )
    conn.close()
    print('MySQL is ready!')
except Exception as e:
    print(f'MySQL not ready: {e}')
    exit(1)
" 2>/dev/null; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ "$RETRY_COUNT" -ge "$MAX_RETRIES" ]; then
        echo "ERROR: MySQL did not become ready after $MAX_RETRIES attempts"
        exit 1
    fi
    echo "  attempt $RETRY_COUNT/$MAX_RETRIES - retrying in 2s..."
    sleep 2
done

# Run database migrations (create/update tables)
echo "Running database migrations (db.py)..."
python3 db.py || echo "WARNING: db.py had issues (may be OK if tables already exist)"

# Start background workers
echo "Starting background workers..."
python3 cron_settings.py &
echo "  - cron_settings.py (PID $!)"

python3 cron_operation.py &
echo "  - cron_operation.py (PID $!)"

python3 tl_outbox_worker.py &
echo "  - tl_outbox_worker.py (PID $!)"

# Start inbox listeners for logged-in accounts
python3 -c "
import os, subprocess, utility as utl
directory = os.path.dirname(os.path.abspath('bot.py'))
try:
    cs = utl.Database().data()
    cs.execute(f\"SELECT uniq_id FROM {utl.mbots} WHERE user_id IS NOT NULL AND status=1\")
    result = cs.fetchall()
    for row in result:
        uniq = row.get('uniq_id')
        if uniq:
            subprocess.Popen(['python3', f'tl_inbox_listener.py', uniq])
            print(f'  - tl_inbox_listener.py {uniq}')
except Exception as e:
    print(f'  (no inbox listeners started: {e})')
" 2>&1 || true

# Start bot.py in FOREGROUND (it blocks on updater.idle())
# This keeps the container alive
echo "Starting bot.py (foreground)..."
exec python3 bot.py
