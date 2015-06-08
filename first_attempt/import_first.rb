require "pg"
require "csv"
require "pry"

def db_connection
  begin
    connection = PG.connect(dbname: "korning")
	   yield(connection)
  ensure
    connection.close
  end
end

def create_employee_legend
  employee_legend = []
  CSV.foreach("sales.csv", headers: true) do |row|
    employee_legend.push(row["employee"])
  end
  employee_legend.uniq!
  employee_legend
end

def create_customer_legend
  customer_legend = []
  CSV.foreach("sales.csv", headers: true) do |row|
    customer_legend.push(row["customer_and_account_no"])
  end
  customer_legend.uniq!
  customer_legend
end

def create_product_legend
  product_legend = []
  CSV.foreach("sales.csv", headers: true) do |row|
    product_legend.push(row["product_name"])
  end
  product_legend.uniq!
  product_legend
end

def create_frequency_legend
  frequency_legend = []
  CSV.foreach("sales.csv", headers: true) do |row|
    frequency_legend.push(row["invoice_frequency"])
  end
  frequency_legend.uniq!
  frequency_legend
end

def import_employee_legend
  db_connection do |conn|
    create_employee_legend.each do |employee_info|

      employee_contact = employee_info.chop.split(" (")
      employee_name = employee_contact[0]
      employee_email = employee_contact[1]

      conn.exec_params("INSERT INTO employee (employee_info, employee_name, employee_email) VALUES ($1, $2, $3)",
        [employee_info, employee_name, employee_email])
    end
  end
end

def import_customer_legend
  db_connection do |conn|
    create_customer_legend.each do |customer_info|

      customer_identifier = customer_info
      customer_info = customer_info.chop.split(" (")
      customer_name = customer_info[0]
      customer_reference = customer_info[1]

      conn.exec_params("INSERT INTO customer (customer_info, customer_name, customer_reference) VALUES ($1, $2, $3)",
        [customer_identifier, customer_name, customer_reference])
    end
  end
end

def import_product_legend
  db_connection do |conn|
    create_product_legend.each do |product|
      conn.exec_params("INSERT INTO product (product_name) VALUES ($1)",
        [product])
    end
  end
end

def import_frequency_legend
  db_connection do |conn|
    create_frequency_legend.each do |frequency|
      conn.exec_params("INSERT INTO invoice_frequency (invoice_frequency) VALUES ($1)",
        [frequency])
    end
  end
end

def create_all
  create_employee_legend
  create_customer_legend
  create_product_legend
  create_frequency_legend
  import_employee_legend
  import_customer_legend
  import_product_legend
  import_frequency_legend
end

create_all

def employee_id(row)
  employee_id = 1 if row["employee"] == "Clancy Wiggum (clancy.wiggum@korning.com)"
  employee_id = 2 if row["employee"] == "Ricky Bobby (ricky.bobby@korning.com)"
  employee_id = 3 if row["employee"] == "Bob Lob (bob.lob@korning.com)"
  employee_id = 4 if row["employee"] == "Willie Groundskeeper (willie.groundskeeper@korning.com)"
  employee_id
end

def customer_id(row)
  customer_id = 1 if row["customer_and_account_no"] == "Motorola (MT928534)"
  customer_id = 2 if row["customer_and_account_no"] == "LG (LG858843)"
  customer_id = 3 if row["customer_and_account_no"] == "HTC (HT925638)"
  customer_id = 4 if row["customer_and_account_no"] == "Nokia (NK881241)"
  customer_id = 5 if row["customer_and_account_no"] == "Samsung (SG373953)"
  customer_id = 6 if row["customer_and_account_no"] == "Apple (AP512452)"
  customer_id
end

def product_id(row)
  product_id = 1 if row["product_name"] == "Chimp Glass"
  product_id = 2 if row["product_name"] == "Baboon Glass"
  product_id = 3 if row["product_name"] == "Orangutan Glass"
  product_id = 4 if row["product_name"] == "Gorilla Glass"
  product_id
end

def frequency_id(row)
  frequency_id = 1 if row["invoice_frequency"] == "Monthly"
  frequency_id = 2 if row["invoice_frequency"] == "Quarterly"
  frequency_id = 3 if row["invoice_frequency"] == "Once"
  frequency_id
end

def create_sales_table
  db_connection do |conn|
    CSV.foreach("sales.csv", headers: true) do |row|
      invoices = conn.exec_params("SELECT invoice_no from sales")
      unless invoices.values.flatten.include?("#{row['invoice_no']}")
        employee_id = employee_id(row)
        customer_id = customer_id(row)
        product_id = product_id(row)
        frequency_id = frequency_id(row)
        conn.exec_params("INSERT INTO sales (employee_id, customer_id, product_id, sale_date,
        sale_amount, units_sold, invoice_no, invoice_frequency_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        [employee_id, customer_id, product_id, row["sale_date"], row["sale_amount"], row["units_sold"],
        row["invoice_no"], frequency_id])
      end
    end
  end
end

create_sales_table
