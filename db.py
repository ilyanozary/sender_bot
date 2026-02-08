# encoding: utf-8

import utility as utl


cs = utl.Database()
cs = cs.data()


def alter_table(cs, sql):
    try:
        cs.execute(sql)
    except:
        pass
    try:
        sql_split = sql.split(" ")
        if sql[0:11] == 'ALTER TABLE':
            if 'UNIQUE' in sql:
                try:
                    cs.execute(f"ALTER TABLE {sql_split[2]} ADD CONSTRAINT {sql_split[4]} UNIQUE({sql_split[4]})")
                except:
                    pass
            sql = sql.replace("ADD", "CHANGE").replace(" UNIQUE", "")
            sql_split_2 = sql.split(f"{sql_split[2]} ")
            sql_split_2[1] = sql_split_2[1].replace(f"{sql_split[4]}", f"{sql_split[4]} {sql_split[4]}")
            sql = f"{sql_split[2]} ".join(sql_split_2)
            cs.execute(sql)
    except:
        pass

##########################
# UPDATE
##########################
# cs.execute(f"UPDATE {utl.mbots} SET uniq_id=phone WHERE status='first_level' OR status='submitted' OR status='restrict'")
# cs.execute(f"UPDATE {utl.mbots} SET status='0' WHERE status='first_level'")
# cs.execute(f"UPDATE {utl.mbots} SET status='1' WHERE status='submitted'")
# cs.execute(f"UPDATE {utl.mbots} SET status='2' WHERE status='restrict'")

# cs.execute(f"UPDATE {utl.egroup} SET status='0' WHERE status='start'")
# cs.execute(f"UPDATE {utl.egroup} SET status='2' WHERE status='end'")
# alter_table(cs, f"ALTER TABLE {utl.egroup} ADD status tinyint(1) NOT NULL DEFAULT 0 AFTER link")

# cs.execute(f"UPDATE {utl.egroup} SET status='1' WHERE status='send'")

# cs.execute(f"UPDATE {utl.orders} SET status='0' WHERE status='start'")
# cs.execute(f"UPDATE {utl.orders} SET status='1' WHERE status='doing'")
# cs.execute(f"UPDATE {utl.orders} SET status='2' WHERE status='end'")

# cs.execute(f"UPDATE {utl.users} SET status='0' WHERE status='user'")
# cs.execute(f"UPDATE {utl.users} SET status='1' WHERE status='admin'")
# cs.execute(f"UPDATE {utl.users} SET status='2' WHERE status='block'")

# cs.execute(f"UPDATE {utl.orders} SET type_users='-1'")
# cs.execute(f"UPDATE {utl.orders} SET type_send='-1'")

# cs.execute(f"RENAME TABLE {utl.orders.replace("gtg", "orders")} TO {utl.orders}")
# cs.execute(f"ALTER TABLE {utl.analyze} CHANGE gtg_id order_id int(11) NOT NULL DEFAULT 0;")
# cs.execute(f"ALTER TABLE {utl.reports} CHANGE gtg_id order_id int(11) NOT NULL DEFAULT 0;")
# cs.execute(f"ALTER TABLE {utl.files} CHANGE gtg_id order_id int(11) NOT NULL DEFAULT 0;")

# limit_per_h: Converted to seconds
##########################
# UPDATE
##########################


alter_table(cs, f"CREATE TABLE IF NOT EXISTS {utl.admin} (id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
alter_table(cs, f"ALTER TABLE {utl.admin} ADD cache varchar(50) DEFAULT NULL AFTER id")
alter_table(cs, f"ALTER TABLE {utl.admin} ADD change_pass tinyint(1) NOT NULL DEFAULT 0 AFTER cache")
alter_table(cs, f"ALTER TABLE {utl.admin} ADD exit_session tinyint(1) NOT NULL DEFAULT 0 AFTER change_pass")
alter_table(cs, f"ALTER TABLE {utl.admin} ADD is_change_profile tinyint(1) NOT NULL DEFAULT 0 AFTER exit_session")
alter_table(cs, f"ALTER TABLE {utl.admin} ADD is_set_username tinyint(1) NOT NULL DEFAULT 0 AFTER is_change_profile")
alter_table(cs, f"ALTER TABLE {utl.admin} ADD api_per_number int(11) NOT NULL DEFAULT 1 AFTER is_set_username")
alter_table(cs, f"ALTER TABLE {utl.admin} ADD limit_per_h int(11) NOT NULL DEFAULT 86400 AFTER api_per_number")
alter_table(cs, f"ALTER TABLE {utl.admin} ADD send_per_h int(11) NOT NULL DEFAULT 16 AFTER limit_per_h")
alter_table(cs, f"ALTER TABLE {utl.admin} ADD account_password varchar(64) DEFAULT NULL AFTER send_per_h")
alter_table(cs, f"ALTER TABLE {utl.admin} ADD disable_inbox tinyint(1) NOT NULL DEFAULT 0 AFTER account_password")


alter_table(cs, f"CREATE TABLE IF NOT EXISTS {utl.analyze} (id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
alter_table(cs, f"ALTER TABLE {utl.analyze} ADD order_id int(11) NOT NULL DEFAULT 0 AFTER id")
alter_table(cs, f"ALTER TABLE {utl.analyze} ADD user_id BIGINT(20) DEFAULT NULL AFTER order_id")
alter_table(cs, f"ALTER TABLE {utl.analyze} ADD username varchar(50) DEFAULT NULL UNIQUE AFTER user_id")
alter_table(cs, f"ALTER TABLE {utl.analyze} ADD group_id varchar(30) DEFAULT NULL AFTER username")
alter_table(cs, f"ALTER TABLE {utl.analyze} ADD is_real tinyint(1) NOT NULL DEFAULT 0 AFTER group_id")
alter_table(cs, f"ALTER TABLE {utl.analyze} ADD is_fake tinyint(1) NOT NULL DEFAULT 0 AFTER is_real")
alter_table(cs, f"ALTER TABLE {utl.analyze} ADD is_phone tinyint(1) NOT NULL DEFAULT 0 AFTER is_fake")
alter_table(cs, f"ALTER TABLE {utl.analyze} ADD is_online tinyint(1) NOT NULL DEFAULT 0 AFTER is_phone")
alter_table(cs, f"ALTER TABLE {utl.analyze} ADD reserved_by varchar(20) DEFAULT NULL AFTER is_online")
alter_table(cs, f"ALTER TABLE {utl.analyze} ADD created_at int(11) NOT NULL DEFAULT 0 AFTER reserved_by")


alter_table(cs, f"CREATE TABLE IF NOT EXISTS {utl.apis} (id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
alter_table(cs, f"ALTER TABLE {utl.apis} ADD api_id varchar(20) DEFAULT NULL UNIQUE AFTER id")
alter_table(cs, f"ALTER TABLE {utl.apis} ADD api_hash varchar(200) DEFAULT NULL UNIQUE AFTER api_id")


alter_table(cs, f"CREATE TABLE IF NOT EXISTS {utl.egroup} (id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
alter_table(cs, f"ALTER TABLE {utl.egroup} ADD type TINYINT(1) NOT NULL DEFAULT 0 AFTER id")
alter_table(cs, f"ALTER TABLE {utl.egroup} ADD user_id BIGINT(20) DEFAULT NULL AFTER type")
alter_table(cs, f"ALTER TABLE {utl.egroup} ADD chat_id varchar(30) DEFAULT NULL AFTER user_id")
alter_table(cs, f"ALTER TABLE {utl.egroup} ADD link varchar(200) DEFAULT NULL AFTER chat_id")
alter_table(cs, f"ALTER TABLE {utl.egroup} ADD status tinyint(1) NOT NULL DEFAULT 0 AFTER link")
alter_table(cs, f"ALTER TABLE {utl.egroup} ADD users_all int(11) NOT NULL DEFAULT 0 AFTER status")
alter_table(cs, f"ALTER TABLE {utl.egroup} ADD users_real int(11) NOT NULL DEFAULT 0 AFTER users_all")
alter_table(cs, f"ALTER TABLE {utl.egroup} ADD users_fake int(11) NOT NULL DEFAULT 0 AFTER users_real")
alter_table(cs, f"ALTER TABLE {utl.egroup} ADD users_has_phone int(11) NOT NULL DEFAULT 0 AFTER users_fake")
alter_table(cs, f"ALTER TABLE {utl.egroup} ADD users_online int(11) NOT NULL DEFAULT 0 AFTER users_has_phone")
alter_table(cs, f"ALTER TABLE {utl.egroup} ADD participants_count int(11) NOT NULL DEFAULT 0 AFTER users_online")
alter_table(cs, f"ALTER TABLE {utl.egroup} ADD participants_online_count int(11) NOT NULL DEFAULT 0 AFTER participants_count")
alter_table(cs, f"ALTER TABLE {utl.egroup} ADD participants_bot_count int(11) NOT NULL DEFAULT 0 AFTER participants_online_count")
alter_table(cs, f"ALTER TABLE {utl.egroup} ADD created_at int(11) NOT NULL DEFAULT 0 AFTER participants_bot_count")
alter_table(cs, f"ALTER TABLE {utl.egroup} ADD updated_at int(11) NOT NULL DEFAULT 0 AFTER created_at")
alter_table(cs, f"ALTER TABLE {utl.egroup} ADD uniq_id varchar(64) DEFAULT NULL AFTER updated_at")


alter_table(cs, f"CREATE TABLE IF NOT EXISTS {utl.files} (id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
alter_table(cs, f"ALTER TABLE {utl.files} ADD order_id int(11) NOT NULL DEFAULT 0 AFTER id")
alter_table(cs, f"ALTER TABLE {utl.files} ADD type_message varchar(20) DEFAULT NULL AFTER id")
alter_table(cs, f"ALTER TABLE {utl.files} ADD message_id int(11) NOT NULL DEFAULT 0 AFTER id")
alter_table(cs, f"ALTER TABLE {utl.files} ADD created_at int(11) NOT NULL DEFAULT 0 AFTER id")
alter_table(cs, f"ALTER TABLE {utl.files} ADD uniq_id varchar(64) DEFAULT NULL AFTER id")


alter_table(cs, f"CREATE TABLE IF NOT EXISTS {utl.orders} (id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD user_id BIGINT(20) DEFAULT NULL AFTER id")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD cats varchar(100) NULL DEFAULT NULL AFTER user_id")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD group_link varchar(200) DEFAULT NULL AFTER cats")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD group_id varchar(20) DEFAULT NULL AFTER group_link")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD count int(11) NOT NULL DEFAULT 0 AFTER group_id")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD max_users int(11) NOT NULL DEFAULT 0 AFTER count")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD count_request int(11) NOT NULL DEFAULT 0 AFTER max_users")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD count_done int(11) NOT NULL DEFAULT 0 AFTER count_request")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD type_users tinyint(1) NOT NULL DEFAULT 0 AFTER count_done")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD type_send tinyint(1) NOT NULL DEFAULT 0 AFTER type_users")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD status tinyint(1) NOT NULL DEFAULT 0 AFTER type_send")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD status_analyze tinyint(1) NOT NULL DEFAULT 0 AFTER status")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD count_acc int(11) NOT NULL DEFAULT 0 AFTER status_analyze")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD count_restrict int(11) NOT NULL DEFAULT 0 AFTER count_acc")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD count_restrict_error int(11) NOT NULL DEFAULT 0 AFTER count_restrict")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD count_report int(11) NOT NULL DEFAULT 0 AFTER count_restrict_error")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD count_accout int(11) NOT NULL DEFAULT 0 AFTER count_report")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD count_usrspam int(11) NOT NULL DEFAULT 0 AFTER count_accout")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD count_userincorrect int(11) NOT NULL DEFAULT 0 AFTER count_usrspam")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD count_other_errors int(11) NOT NULL DEFAULT 0 AFTER count_userincorrect")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD send_per_h int(11) NOT NULL DEFAULT 0 AFTER count_other_errors")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD created_at int(11) NOT NULL DEFAULT 0 AFTER send_per_h")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD updated_at int(11) NOT NULL DEFAULT 0 AFTER created_at")
alter_table(cs, f"ALTER TABLE {utl.orders} ADD uniq_id varchar(64) DEFAULT NULL AFTER updated_at")


alter_table(cs, f"CREATE TABLE IF NOT EXISTS {utl.mbots} (id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD creator_user_id BIGINT(20) DEFAULT NULL AFTER id")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD cat_id int(11) NOT NULL DEFAULT 0 AFTER creator_user_id")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD phone varchar(20) DEFAULT NULL UNIQUE AFTER cat_id")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD user_id BIGINT(20) DEFAULT NULL AFTER phone")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD status tinyint(1) NOT NULL DEFAULT 0 AFTER user_id")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD end_restrict int(11) NOT NULL DEFAULT 0 AFTER status")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD last_order_at int(11) NOT NULL DEFAULT 0 AFTER end_restrict")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD last_leave_at int(11) NOT NULL DEFAULT 0 AFTER last_order_at")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD last_delete_chats_at int(11) NOT NULL DEFAULT 0 AFTER last_leave_at")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD api_id varchar(20) DEFAULT NULL AFTER last_delete_chats_at")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD api_hash varchar(200) DEFAULT NULL AFTER api_id")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD code int(11) DEFAULT NULL AFTER api_hash")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD password varchar(100) DEFAULT NULL AFTER code")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD is_change_pass tinyint(1) NOT NULL DEFAULT 0 AFTER password")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD change_pass_at int(11) NOT NULL DEFAULT 0 AFTER is_change_pass")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD is_exit_session tinyint(1) NOT NULL DEFAULT 0 AFTER change_pass_at")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD exit_session_at int(11) NOT NULL DEFAULT 0 AFTER is_exit_session")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD is_change_profile tinyint(1) NOT NULL DEFAULT 0 AFTER exit_session_at")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD is_set_username tinyint(1) NOT NULL DEFAULT 0 AFTER is_change_profile")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD created_at int(11) NOT NULL DEFAULT 0 AFTER is_set_username")
alter_table(cs, f"ALTER TABLE {utl.mbots} ADD uniq_id varchar(64) DEFAULT NULL AFTER created_at")


alter_table(cs, f"CREATE TABLE IF NOT EXISTS {utl.reports} (id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
alter_table(cs, f"ALTER TABLE {utl.reports} ADD order_id int(11) NOT NULL DEFAULT 0 AFTER id")
alter_table(cs, f"ALTER TABLE {utl.reports} ADD bot_id int(11) NOT NULL DEFAULT 0 AFTER order_id")
alter_table(cs, f"ALTER TABLE {utl.reports} ADD user_id BIGINT(20) DEFAULT NULL AFTER bot_id")
alter_table(cs, f"ALTER TABLE {utl.reports} ADD username varchar(50) DEFAULT NULL AFTER user_id")
alter_table(cs, f"ALTER TABLE {utl.reports} ADD group_id varchar(30) DEFAULT NULL AFTER username")
alter_table(cs, f"ALTER TABLE {utl.reports} ADD status tinyint(1) NOT NULL DEFAULT 0 AFTER group_id")
alter_table(cs, f"ALTER TABLE {utl.reports} ADD created_at int(11) NOT NULL DEFAULT 0 AFTER status")


alter_table(cs, f"CREATE TABLE IF NOT EXISTS {utl.usedaccs} (id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
alter_table(cs, f"ALTER TABLE {utl.usedaccs} ADD order_id int(11) NOT NULL DEFAULT 0 AFTER id")
alter_table(cs, f"ALTER TABLE {utl.usedaccs} ADD bot_id int(11) NOT NULL DEFAULT 0 AFTER order_id")
alter_table(cs, f"ALTER TABLE {utl.usedaccs} ADD created_at int(11) NOT NULL DEFAULT 0 AFTER bot_id")


alter_table(cs, f"CREATE TABLE IF NOT EXISTS {utl.cats} (id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
alter_table(cs, f"ALTER TABLE {utl.cats} ADD name varchar(50) DEFAULT NULL UNIQUE AFTER id")
alter_table(cs, f"UPDATE {utl.mbots} SET cat_id=1 WHERE cat_id=0")


alter_table(cs, f"CREATE TABLE IF NOT EXISTS {utl.users} (id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
alter_table(cs, f"ALTER TABLE {utl.users} ADD user_id BIGINT(20) DEFAULT NULL UNIQUE AFTER id")
alter_table(cs, f"ALTER TABLE {utl.users} ADD status tinyint(1) NOT NULL DEFAULT 0 AFTER user_id")
alter_table(cs, f"ALTER TABLE {utl.users} ADD step varchar(50) DEFAULT NULL AFTER status")
alter_table(cs, f"ALTER TABLE {utl.users} ADD created_at int(11) DEFAULT NULL AFTER step")
alter_table(cs, f"ALTER TABLE {utl.users} ADD uniq_id varchar(64) DEFAULT NULL AFTER created_at")


cs.execute(f"SELECT * FROM {utl.admin}")
row_admin = cs.fetchone()
if row_admin is None:
    cs.execute(f"INSERT INTO {utl.admin} (id) VALUES (1)")
cs.execute(f"SELECT * FROM {utl.cats}")
row_cats = cs.fetchone()
if row_cats is None:
    cs.execute(f"INSERT INTO {utl.cats} (id,name) VALUES (1,'default')")

##########################
# Inbox / Outbox for incoming messages & admin replies
##########################
alter_table(cs, f"CREATE TABLE IF NOT EXISTS {utl.inbox} (id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
alter_table(cs, f"ALTER TABLE {utl.inbox} ADD mbot_id int(11) NOT NULL DEFAULT 0 AFTER id")
alter_table(cs, f"ALTER TABLE {utl.inbox} ADD from_id BIGINT(20) DEFAULT NULL AFTER mbot_id")
alter_table(cs, f"ALTER TABLE {utl.inbox} ADD from_username varchar(255) DEFAULT NULL AFTER from_id")
alter_table(cs, f"ALTER TABLE {utl.inbox} ADD text text DEFAULT NULL AFTER from_username")
alter_table(cs, f"ALTER TABLE {utl.inbox} ADD reply_to_outgoing_id BIGINT(20) DEFAULT NULL AFTER text")
alter_table(cs, f"ALTER TABLE {utl.inbox} ADD thread_id varchar(64) DEFAULT NULL AFTER reply_to_outgoing_id")
alter_table(cs, f"ALTER TABLE {utl.inbox} ADD created_at int(11) NOT NULL DEFAULT 0 AFTER thread_id")
alter_table(cs, f"ALTER TABLE {utl.inbox} ADD processed tinyint(1) NOT NULL DEFAULT 0 AFTER created_at")
# ensure we can store the original incoming Telethon message id so replies can target it
alter_table(cs, f"ALTER TABLE {utl.inbox} ADD from_message_id BIGINT(20) DEFAULT NULL AFTER created_at")
alter_table(cs, f"ALTER TABLE {utl.inbox} ADD message_id BIGINT(20) DEFAULT NULL AFTER from_message_id")
# store sender's first/last name for richer admin UI
alter_table(cs, f"ALTER TABLE {utl.inbox} ADD from_first_name varchar(150) DEFAULT NULL AFTER from_username")
alter_table(cs, f"ALTER TABLE {utl.inbox} ADD from_last_name varchar(150) DEFAULT NULL AFTER from_first_name")

# table to keep track of blocked senders per mbot (so admins can mute listening for specific users)
alter_table(cs, f"CREATE TABLE IF NOT EXISTS {utl.inbox_blocked} (id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
alter_table(cs, f"ALTER TABLE {utl.inbox_blocked} ADD mbot_id int(11) NOT NULL DEFAULT 0 AFTER id")
alter_table(cs, f"ALTER TABLE {utl.inbox_blocked} ADD from_id BIGINT(20) DEFAULT NULL AFTER mbot_id")
alter_table(cs, f"ALTER TABLE {utl.inbox_blocked} ADD from_username varchar(255) DEFAULT NULL AFTER from_id")
alter_table(cs, f"ALTER TABLE {utl.inbox_blocked} ADD created_at int(11) NOT NULL DEFAULT 0 AFTER from_username")

alter_table(cs, f"CREATE TABLE IF NOT EXISTS {utl.outbox} (id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
alter_table(cs, f"ALTER TABLE {utl.outbox} ADD mbot_id int(11) NOT NULL DEFAULT 0 AFTER id")
alter_table(cs, f"ALTER TABLE {utl.outbox} ADD target_id BIGINT(20) DEFAULT NULL AFTER mbot_id")
alter_table(cs, f"ALTER TABLE {utl.outbox} ADD target_username varchar(255) DEFAULT NULL AFTER target_id")
alter_table(cs, f"ALTER TABLE {utl.outbox} ADD reply_to_message_id BIGINT(20) DEFAULT NULL AFTER target_username")
alter_table(cs, f"ALTER TABLE {utl.outbox} ADD text text DEFAULT NULL AFTER reply_to_message_id")
alter_table(cs, f"ALTER TABLE {utl.outbox} ADD attachments varchar(500) DEFAULT NULL AFTER text")
alter_table(cs, f"ALTER TABLE {utl.outbox} ADD status varchar(20) DEFAULT 'new' AFTER attachments")
alter_table(cs, f"ALTER TABLE {utl.outbox} ADD locked_by varchar(64) DEFAULT NULL AFTER status")
alter_table(cs, f"ALTER TABLE {utl.outbox} ADD locked_until int(11) DEFAULT NULL AFTER locked_by")
alter_table(cs, f"ALTER TABLE {utl.outbox} ADD attempts int(11) NOT NULL DEFAULT 0 AFTER locked_until")
alter_table(cs, f"ALTER TABLE {utl.outbox} ADD last_error varchar(255) DEFAULT NULL AFTER attempts")
alter_table(cs, f"ALTER TABLE {utl.outbox} ADD created_at int(11) NOT NULL DEFAULT 0 AFTER last_error")
alter_table(cs, f"ALTER TABLE {utl.outbox} ADD updated_at int(11) DEFAULT NULL AFTER created_at")
alter_table(cs, f"ALTER TABLE {utl.outbox} ADD reply_to_inbox_id int(11) DEFAULT NULL AFTER reply_to_message_id")
alter_table(cs, f"ALTER TABLE {utl.outbox} ADD sent_message_id int(11) DEFAULT NULL AFTER updated_at")
alter_table(cs, f"ALTER TABLE {utl.outbox} ADD max_attempts int(11) NOT NULL DEFAULT 3 AFTER attempts")