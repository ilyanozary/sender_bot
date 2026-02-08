import os, subprocess, utility as utl

directory = os.path.dirname(os.path.abspath(__file__))
filename = str(os.path.basename(__file__))

utl.get_params_pids_by_full_script_name(script_names=[f"{directory}/{filename}"], is_kill_proccess=True)


subprocess.Popen([utl.python_version, f"{directory}/bot.py"])
subprocess.Popen([utl.python_version, f"{directory}/cron_settings.py"])
subprocess.Popen([utl.python_version, f"{directory}/cron_operation.py"])
# start outbox worker
subprocess.Popen([utl.python_version, f"{directory}/tl_outbox_worker.py"])

# start inbox listeners for each logged-in mbot (user_id IS NOT NULL and status=1)
try:
	cs = utl.Database().data()
	cs.execute(f"SELECT uniq_id FROM {utl.mbots} WHERE user_id IS NOT NULL AND status=1")
	result = cs.fetchall()
	for row in result:
		uniq = row.get('uniq_id')
		if uniq:
			try:
				subprocess.Popen([utl.python_version, f"{directory}/tl_inbox_listener.py", uniq])
			except Exception:
				pass
except Exception:
	# if DB not available or query fails, continue without starting listeners
	pass
