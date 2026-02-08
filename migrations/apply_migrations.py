"""
Safe migration runner for the small schema changes required by chenge_base.

Usage (dry-run):
    python3 migrations/apply_migrations.py

To actually apply changes you must set the environment variable `MIGRATIONS_RUN=1` OR pass --apply:
    MIGRATIONS_RUN=1 python3 migrations/apply_migrations.py
    or
    python3 migrations/apply_migrations.py --apply

The script uses the project's `utility` module to read real table names and DB connection.
It will NOT modify anything unless the explicit guard is provided.
"""

import os
import sys
import argparse
import traceback
import re
import os
import pymysql

# We avoid importing `utility` because it pulls heavy dependencies (telegram, etc.).
# Instead, read table name constants from `utility.py` via text parsing and use
# `config.py` for DB connection parameters.
from config import host_db, user_db, passwd_db, database, port


def read_table_names_from_utility(path=None):
    if path is None:
        path = os.path.join(os.path.dirname(__file__), '..', 'utility.py')
        path = os.path.abspath(path)
    names = {}
    pattern = re.compile(r"^(\w+)\s*=\s*'([^']+)'\s*$")
    try:
        with open(path, 'r') as f:
            for line in f:
                m = pattern.match(line.strip())
                if m:
                    names[m.group(1)] = m.group(2)
    except Exception as e:
        raise RuntimeError(f"Failed to read utility.py for table names: {e}")
    return names


def get_statements():
    names = read_table_names_from_utility()
    if 'analyze' not in names or 'files' not in names:
        raise RuntimeError('Required table names (analyze, files) not found in utility.py')
    analyze_table = names['analyze']
    files_table = names['files']
    stmts = []
    stmts.append(f"ALTER TABLE `{analyze_table}` ADD COLUMN `batch` int(11) NOT NULL DEFAULT 1 AFTER `created_at`;")
    stmts.append(f"ALTER TABLE `{files_table}` ADD COLUMN `batch` int(11) NOT NULL DEFAULT 1 AFTER `uniq_id`;")
    stmts.append(f"ALTER TABLE `{files_table}` ADD COLUMN `msg_index` tinyint(1) NOT NULL DEFAULT 1 AFTER `batch`;")
    # add inbox message id columns if not present
    stmts.append(f"ALTER TABLE `{names.get('inbox','')}` ADD COLUMN `from_message_id` int(11) DEFAULT NULL AFTER `thread_id`;")
    stmts.append(f"ALTER TABLE `{names.get('inbox','')}` ADD COLUMN `message_id` int(11) DEFAULT NULL AFTER `from_message_id`;")
    return stmts
    


def apply(statements):
    conn = pymysql.connect(host=host_db, user=user_db, password=passwd_db, database=database, port=port, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor, autocommit=True)
    cs = conn.cursor()
    results = []
    for s in statements:
        try:
            cs.execute(s)
            results.append((s, True, None))
        except Exception as e:
            results.append((s, False, str(e)))
    try:
        cs.close()
        conn.close()
    except:
        pass
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true', help='Actually run migrations (same as setting MIGRATIONS_RUN=1)')
    args = parser.parse_args()

    stmts = get_statements()

    print('Planned statements:')
    for s in stmts:
        print('\n' + s)

    do_apply = args.apply or os.environ.get('MIGRATIONS_RUN') == '1'
    if not do_apply:
        print('\nDRY RUN: no statements were executed. To apply set MIGRATIONS_RUN=1 or pass --apply.')
        return 0

    print('\nApplying statements now...')
    try:
        results = apply(stmts)
        for s, ok, err in results:
            if ok:
                print('OK: ', s)
            else:
                print('FAILED:', s)
                print('  ->', err)
        print('\nDone. Review results above.')
        return 0
    except Exception:
        print('Unexpected error while applying migrations:')
        traceback.print_exc()
        return 2


if __name__ == '__main__':
    sys.exit(main())
