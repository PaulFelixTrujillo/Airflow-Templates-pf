
-- Table: bootcampdb_pf.user_purchase

DROP TABLE bootcampdb_pf.user_purchase;

CREATE TABLE IF NOT EXISTS bootcampdb_pf.user_purchase
(
    invoice_number varchar (10),
    stock_code varchar (20),
    detail varchar (1000),
    quality int,
    invoice_date timestamp,
    unit_price numeric(8,3)
    customer_id int,
    country varchar(20) 
);