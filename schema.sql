/*
  sales.csv headers
  -----------------
  employee,
  customer_and_account_no,
  product_name,
  sale_date,
  sale_amount,
  units_sold,
  invoice_no,
  invoice_frequency
*/

DROP TABLE employees CASCADE;
DROP TABLE customers CASCADE;
DROP TABLE products CASCADE;
DROP TABLE invoices CASCADE;

CREATE TABLE employees(
  id SERIAL PRIMARY KEY NOT NULL,
  first_name VARCHAR(255),
  last_name VARCHAR(255),
  email VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE customers(
  id SERIAL PRIMARY KEY NOT NULL,
  name VARCHAR(255) NOT NULL UNIQUE,
  account_number VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE products(
  id SERIAL PRIMARY KEY NOT NULL,
  name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE invoices(
  id SERIAL PRIMARY KEY,
  invoice_number INTEGER NOT NULL,
  invoice_frequency VARCHAR(255),
  employee_id INTEGER REFERENCES employees(id),
  customer_id INTEGER REFERENCES customers(id),
  product_id INTEGER REFERENCES products(id),
  units_sold INTEGER,
  sale_date DATE,
  sale_amount MONEY
);
