-- liquibase formatted sql

--changeset balance:1
CREATE TABLE balance (
    id INTEGER PRIMARY KEY,
    balance DECIMAL(15,4) NOT NULL,
    bank TEXT,
    date DATETIME,
    FOREIGN KEY(bank) REFERENCES banks(bank)
);
--rollback DROP TABLE balance;
