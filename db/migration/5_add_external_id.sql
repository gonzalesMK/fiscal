-- liquibase formatted sql
-- changeset fiscal:12
ALTER TABLE transactions ADD COLUMN external_id TEXT;
--rollback DROP COLUMN IF EXISTS nfes.description;

