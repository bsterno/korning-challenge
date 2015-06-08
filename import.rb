require "csv"
require "pg"
require "pry"

def db_connection
  begin
    connection = PG.connect(dbname: "korning")
    yield(connection)
  ensure
    connection.close
  end
end

def insert_items(sql, items)
  items.each do |item|
    db_connection do |connection|
      connection.exec(sql, item)
    end
  end
end

def get_employee_id(email)
  sql = "SELECT id FROM employees WHERE email = $1;"
  id = db_connection do |connection|
    connection.exec(sql, [email])
  end
  id.to_a.first["id"]
end


def get_product_id(name)
  sql = "SELECT id FROM products WHERE name = $1;"
  id = db_connection do |connection|
    connection.exec(sql, [name])
  end
  id.to_a.first["id"]
end

def get_customer_id(name)
  sql = "SELECT id FROM customers WHERE name = $1;"
  id = db_connection do |connection|
    connection.exec(sql, [name])
  end
  id.to_a.first["id"]
end

system('psql korning < schema.sql')

## Insert employees into database

employees = []
customers = []
products = []

csv_options = { headers: true, header_converters: :symbol }
CSV.foreach("sales.csv", csv_options) do |row|
  employee = row[:employee]
  employee.delete!('()')
  first_name, last_name, email = employee.split
  employees << [first_name, last_name, email]

  customer = row[:customer_and_account_no]
  customer.delete!('()')
  name, account_number = customer.split
  customers << [name, account_number]

  products << [row[:product_name]]
end

sql = <<-SQL
  INSERT INTO employees(
    first_name,
    last_name,
    email
  ) VALUES ($1, $2, $3);
SQL

insert_items(sql, employees.uniq)

sql = <<-SQL
  INSERT INTO customers(
    name,
    account_number
  ) VALUES ($1, $2);
SQL

insert_items(sql, customers.uniq)

## Insert products

sql = "INSERT INTO products(name) VALUES ($1);"
insert_items(sql, products.uniq)

## Insert invoices
invoices = []
CSV.foreach("sales.csv", csv_options) do |row|
  sale_amount = row[:sale_amount]
  invoice_number = row[:invoice_no]
  sale_date = row[:sale_date]
  units_sold = row[:units_sold]
  invoice_frequency = row[:invoice_frequency]

## Finding employee_id
  employee = row[:employee]
  employee.delete!('()')
  first_name, last_name, email = employee.split
  employee_id = get_employee_id(email)

## Finding product_id
  product = row[:product_name]
  product_id = get_product_id(product)

## Finding customer_id
  customer = row[:customer_and_account_no]
  customer.delete!('()')
  customer_name, customer_id = customer.split
  customer_id = get_customer_id(customer_name)

## Inserting all data into invoices table
  invoices << [
    sale_amount,
    invoice_number,
    sale_date,
    units_sold,
    invoice_frequency,
    employee_id,
    product_id,
    customer_id
  ]
end

sql = <<-SQL
  INSERT INTO invoices(
    sale_amount,
    invoice_number,
    sale_date,
    units_sold,
    invoice_frequency,
    employee_id,
    product_id,
    customer_id
  ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
SQL

insert_items(sql, invoices)
