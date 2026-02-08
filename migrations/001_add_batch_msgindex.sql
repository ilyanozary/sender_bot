-- Dry-run SQL: add batch/msg_index columns
-- This file is for review. Do NOT execute blindly on production.

ALTER TABLE `{analyze}` ADD COLUMN `batch` int(11) NOT NULL DEFAULT 1 AFTER `created_at`;

ALTER TABLE `{files}` ADD COLUMN `batch` int(11) NOT NULL DEFAULT 1 AFTER `uniq_id`;
ALTER TABLE `{files}` ADD COLUMN `msg_index` tinyint(1) NOT NULL DEFAULT 1 AFTER `batch`;
