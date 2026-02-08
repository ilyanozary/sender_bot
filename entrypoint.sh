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

# Start the application
echo "Starting application via run.py..."
exec python3 run.py
