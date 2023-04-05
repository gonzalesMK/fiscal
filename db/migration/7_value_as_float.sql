-- liquibase formatted sql
-- changeset fiscal:15
ALTER TABLE transactions ADD COLUMN currency NUMERIC(25, 10);
UPDATE transactions set currency = CAST(value as NUMERIC(25, 10));
ALTER TABLE transactions DROP COLUMN value;
ALTER TABLE transactions ADD COLUMN value NUMERIC(25, 10);
UPDATE transactions set value = currency;
ALTER TABLE transactions DROP COLUMN currency;
