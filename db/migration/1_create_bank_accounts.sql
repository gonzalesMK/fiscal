-- liquibase formatted sql
-- changeset fiscal:1
CREATE TABLE banks
(
    bank TEXT PRIMARY KEY,
    description TEXT);

CREATE TABLE companies
(
    name text PRIMARY KEY,
    cnpj TEXT,
    default_category TEXT,
    descricao TEXT,
    UNIQUE(cnpj)
);

CREATE TABLE categories
(
    category TEXT PRIMARY KEY
);

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
    FOREIGN KEY(counterpart_name) REFERENCES company_naming(nickname),
    FOREIGN KEY(category) REFERENCES categories(category)
);

CREATE TABLE nfes
(
    codigo_acesso TEXT PRIMARY KEY,
    emissor TEXT,
    dt_emissao DATETIME,
    valor_liquido FLOAT,
    valor_total FLOAT,
    validated boolean DEFAULT false,

    FOREIGN KEY(emissor) REFERENCES companies(name));


CREATE TABLE validations
(
    transacao INTEGER,
    codigo_acesso text,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(codigo_acesso, transacao),
    FOREIGN KEY(codigo_acesso) REFERENCES nfes(codigo_acesso),
    FOREIGN KEY(transacao) REFERENCES transactions(id));

CREATE TABLE company_naming
(
    nickname text PRIMARY KEY,
    name text,
    FOREIGN KEY(name) REFERENCES companies(name)
);

INSERT INTO "main"."categories" (category)
VALUES
    (''),
    ('frete'),
    ('insumos'),
    ('segurança'),
    ('serviços 3º'),
    ('contador'),
    ('sistemas'),
    ('salários'),
    ('compras'),
    ('ignorar'),
    ('bancos'),
    ('imposto'),
    ('marketing'),
    ('embalagens'),
    ('entrada');

INSERT INTO "main"."banks" (bank, description)
VALUES
    ('bb', 'Banco do Brasil');

