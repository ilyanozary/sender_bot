#!/usr/bin/env python3
"""
Kill all running processes for this project by script filename.

Targets (by basename):
 - bot.py
 - tl_inbox_listener.py
 - tl_outbox_worker.py
 - cron_settings.py
 - cron_operation.py

This script finds processes whose command-line contains any of the above
filenames (matching basenames) and tries to terminate them gracefully. If a
process doesn't exit within a short timeout it will be force-killed.
"""
import os
import time
import signal
import psutil

TARGET_SCRIPTS = [
    'bot.py',
    'tl_inbox_listener.py',
    'tl_outbox_worker.py',
    'cron_settings.py',
    'cron_operation.py',
    'tl_run_account.py',
    'run.py'
]

def matches_script(cmdline):
    if not cmdline:
        return False
    for part in cmdline:
        try:
            name = os.path.basename(part)
        except Exception:
            name = part
        if name in TARGET_SCRIPTS:
            return True
    return False

def find_targets(exclude_pid=None):
    procs = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            pid = proc.info.get('pid')
            if exclude_pid is not None and pid == exclude_pid:
                continue
            cmdline = proc.info.get('cmdline') or []
            if matches_script(cmdline):
                procs.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return procs

def terminate_procs(procs, grace=5):
    if not procs:
        print('No matching processes found.')
        return
    print(f'Found {len(procs)} processes to terminate:')
    for p in procs:
        try:
            print(f' - PID {p.pid}: {p.cmdline()}')
        except Exception:
            print(f' - PID {p.pid}: (cmdline unavailable)')

    # First attempt graceful terminate
    for p in procs:
        try:
            p.terminate()
        except Exception as e:
            print(f'Warning: failed to terminate PID {p.pid}: {e}')

    # wait for processes to exit
    gone, alive = psutil.wait_procs(procs, timeout=grace)
    if alive:
        print(f'{len(alive)} processes did not exit after {grace}s, killing...')
        for p in alive:
            try:
                print(f' - Killing PID {p.pid}')
                p.kill()
            except Exception as e:
                print(f'Error killing PID {p.pid}: {e}')
        # final wait
        psutil.wait_procs(alive, timeout=3)
    else:
        print('All processes terminated gracefully.')

def main():
    exclude_pid = os.getpid()
    print('kill_robot_processes: scanning for processes...')
    procs = find_targets(exclude_pid=exclude_pid)
    if not procs:
        print('No robot processes are currently running.')
        return
    # confirm with user
    #try:
    #    ans = input('Terminate listed processes? [y/N]: ').strip().lower()
    #except Exception:
    #    ans = 'n'
    #if ans != 'y':
    #    print('Aborted by user.')
    #    return
    terminate_procs(procs)

if __name__ == '__main__':
    main()
