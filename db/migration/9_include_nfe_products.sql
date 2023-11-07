-- liquibase formatted sql

--changeset nfe_products:9
CREATE TABLE PRODUCTS (
    id INTEGER PRIMARY KEY,
    codigo_acesso TEXT,
    name TEXT, 
    unit_value DECIMAL(10,2),
    total_value DECIMAL(10,2),
    quantity INTEGER,
    unity TEXT,
    dt_emissao DATETIME,
    FOREIGN KEY(codigo_acesso) REFERENCES NFES(codigo_acesso)
);
--rollback DROP TABLE PRODUCTS;
