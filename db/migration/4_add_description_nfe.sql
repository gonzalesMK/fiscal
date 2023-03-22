-- liquibase formatted sql
-- changeset fiscal:12
ALTER TABLE nfes ADD COLUMN description TEXT;
--rollback DROP COLUMN IF EXISTS nfes.description;

