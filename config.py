import os

admins = [7459925039,132940913]  # Admin UserIDs
token = os.getenv("PV_BOT_TOKEN", "8569302405:AAHuvCca8pS6OPRwmV46kC-K7ix_RtSc6jc")  # Bot Token

# Database settings (override via environment variables)
host_db = os.getenv("PV_DB_HOST", "localhost")
database = os.getenv("PV_DB_NAME", "pv_bot")  # Database Name
user_db = os.getenv("PV_DB_USER", "becherostam")  # Database Username
passwd_db = os.getenv("PV_DB_PASS", "123Q")  # Database Password
try:
	port = int(os.getenv("PV_DB_PORT", "3306"))
except Exception:
	port = 3306

# Python interpreter to spawn sub-processes with
python_version = os.getenv("PV_PYTHON", "python3")   	 
