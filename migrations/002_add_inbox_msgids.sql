-- Add columns to store Telethon incoming message IDs in inbox
-- Adds `from_message_id` and `message_id` (nullable) so listeners can store
-- Telethon message.id for replies and read-acks.

ALTER TABLE `pvs_inbox_q1w2e3r4t5y6u7i8o9p0` ADD COLUMN `from_message_id` int(11) DEFAULT NULL AFTER `thread_id`;
ALTER TABLE `pvs_inbox_q1w2e3r4t5y6u7i8o9p0` ADD COLUMN `message_id` int(11) DEFAULT NULL AFTER `from_message_id`;
