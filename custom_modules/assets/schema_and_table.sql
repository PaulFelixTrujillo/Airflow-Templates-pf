DROP TABLE second_deliverable.user_purchase;

CREATE SCHEMA IF NOT EXISTS second_deliverable;

CREATE TABLE IF NOT EXISTS second_deliverable.user_purchase (
    invoice_number varchar (10),
    stock_code varchar(20),
    detail varchar (1000),
    quality int,
    invoice_date timestamp,
    unit_price numeric(8,3)
    customer_id int,
    country varchar(20)  
);

/*TRUNCATE TABLE second_deliverable.user_purchase;*/
