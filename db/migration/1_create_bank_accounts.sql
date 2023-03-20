-- liquibase formatted sql
-- changeset fiscal:1
CREATE TABLE banks
(
    bank TEXT PRIMARY KEY,
    description TEXT);
-- rollback drop table banks

-- changeset fiscal:2
CREATE TABLE companies
(
    name text PRIMARY KEY,
    cnpj TEXT,
    default_category TEXT,
    descricao TEXT,
    UNIQUE(cnpj)
);
-- rollback drop table companies

-- changeset fiscal:3
CREATE TABLE categories
(
    category TEXT PRIMARY KEY
);
-- changeset fiscal:4
CREATE TABLE transactions
(
    id INTEGER PRIMARY KEY,
    bank TEXT,
    date DATETIME,
    entry_type text,
    transaction_type text,
    category text,
    description text,
    value text,
    counterpart_name text,
    validated boolean NOT NULL,
    FOREIGN KEY(bank) REFERENCES banks(bank),
    FOREIGN KEY(counterpart_name) REFERENCES companies(name),
    FOREIGN KEY(category) REFERENCES categories(category)
);
-- rollback drop table transactions

-- changeset fiscal:5
CREATE TABLE nfes
(
    codigo_acesso TEXT PRIMARY KEY,
    emissor TEXT,
    dt_emissao DATETIME,
    valor_liquido FLOAT,
    valor_total FLOAT,
    validated boolean DEFAULT false,

    FOREIGN KEY(emissor) REFERENCES companies(name));
-- rollback drop table nfes


-- changeset fiscal:6
CREATE TABLE validations
(
    transacao INTEGER,
    codigo_acesso text,
    PRIMARY KEY(codigo_acesso, transacao),
    FOREIGN KEY(codigo_acesso) REFERENCES nfes(codigo_acesso),
    FOREIGN KEY(transacao) REFERENCES transactions(id));
-- rollback drop table validations

-- changeset fiscal:7
CREATE TABLE company_naming
(
    nickname text PRIMARY KEY,
    name text,
    FOREIGN KEY(name) REFERENCES companies(name)
);
-- rollback drop table companies

-- changeset fiscal:8
INSERT INTO "main"."categories" (category)
VALUES
    ('Frete'),
    ('Insumos'),
    ('Segurança'),
    ('Serviços 3º'),
    ('Contador'),
    ('Sistemas'),
    ('Salários'),
    ('Compras'),
    ('Ignorar'),
    ('Bancos'),
    ('Imposto'),
    ('Marketing');

-- changeset fiscal:9
INSERT INTO "main"."categories" (category)
VALUES
    ('Embalagens');

