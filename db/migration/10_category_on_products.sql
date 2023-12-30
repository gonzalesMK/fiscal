-- liquibase formatted sql

--changeset nfe_products:10
ALTER TABLE PRODUCTS RENAME TO products_pricing;
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT, 
    category TEXT
);
--rollback ALTER TABLE products_pricing DROP CONSTRAINT fk_product;
--rollback DROP TABLE product_categories;
--rollback ALTER TABLE products_pricing RENAME TO PRODUCTS
