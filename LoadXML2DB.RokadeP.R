# Load packages
pacman::p_load(XML,  RMySQL, dplyr, tibble)

# Create a new SQLite database connection
db <- dbConnect(RMySQL::MySQL(),
                 dbname = "sql5698177",
                 host = "sql5.freemysqlhosting.net",
                 user = "sql5698177",
                 password = "MuHk4ATCUX")

# Drop existing tables
dbExecute(db, "DROP TABLE IF EXISTS sales")
dbExecute(db, "DROP TABLE IF EXISTS products")
dbExecute(db, "DROP TABLE IF EXISTS reps")
dbExecute(db, "DROP TABLE IF EXISTS customers")

# Create the 'products' table
dbExecute(db, "CREATE TABLE products (
  product_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
)")

# Create the 'reps' table
dbExecute(db, "CREATE TABLE reps (
  rep_id INTEGER PRIMARY KEY,
  first_name TEXT NOT NULL,
  sur_name TEXT NOT NULL,
  territory TEXT NOT NULL,
  commission REAL NOT NULL
)")

# Create the 'customers' table
dbExecute(db, "CREATE TABLE customers (
  customer_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  country TEXT NOT NULL
)")

# Create the 'sales' table
dbExecute(db, "CREATE TABLE sales (
  sales_id INTEGER PRIMARY KEY,
  rep_id INTEGER NOT NULL,
  customer_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  date TEXT NOT NULL,
  qty INTEGER NOT NULL,
  total_amount REAL NOT NULL,
  currency TEXT NOT NULL,
  FOREIGN KEY (rep_id) REFERENCES reps(rep_id),
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  FOREIGN KEY (product_id) REFERENCES products(product_id)
)")

# Check if all the tables have been created
dbGetQuery(db, "SHOW TABLES")

# Check table products attributes
dbGetQuery(db, "SHOW COLUMNS FROM products")

# Check table reps attributes
dbGetQuery(db, "SHOW COLUMNS FROM reps")

# Check table customers attributes
dbGetQuery(db, "SHOW COLUMNS FROM customers")

# Check table sales attributes
dbGetQuery(db, "SHOW COLUMNS FROM sales")

# Function to load XML files
loadXMLfiles <- function(directory, pattern) {
  file_paths <- list.files(directory, pattern = pattern, full.names = TRUE)
  return(file_paths)
}

# Function to write XML files
writeXMLfiles <- function(xml_path, tag){
  xml_data <- xmlParse(xml_path)
  
  if (tag == "//rep"){
    rep_id <- xpathSApply(xml_data, tag, xmlGetAttr, "rID")
    first_name <- xpathSApply(xml_data, paste(tag, "/name/first", sep=""),
                              xmlValue)
    sur_name <- xpathSApply(xml_data, paste(tag, "/name/sur", sep=""),
                            xmlValue)
    territory <- xpathSApply(xml_data, paste(tag, "/territory", sep=""),
                             xmlValue)
    commission <- xpathSApply(xml_data, paste(tag, "/commission", sep=""),
                              xmlValue)
    
    df <- data.frame(
      rep_id = as.numeric(gsub("r", "", rep_id)),
      first_name = first_name,
      sur_name = sur_name,
      territory = territory,
      commission = commission
    )
  }
  else if (tag == "//txn"){
    rep_id <- xpathSApply(xml_data, tag, xmlGetAttr, "repID")
    customer <- xpathSApply(xml_data, paste(tag, "/customer", sep=""),
                            xmlValue)
    country <- xpathSApply(xml_data, paste(tag, "/country", sep=""),
                           xmlValue)
    date <- xpathSApply(xml_data, paste(tag, "/sale/date", sep=""),
                        xmlValue)
    product <- xpathSApply(xml_data, paste(tag, "/sale/product", sep=""),
                           xmlValue)
    qty <- xpathSApply(xml_data, paste(tag, "/sale/qty", sep=""),
                       xmlValue)
    total_amount <- xpathSApply(xml_data, paste(tag, "/sale/total", sep=""),
                                xmlValue)
    currency <- xpathSApply(xml_data, paste(tag, "/sale/total", sep=""),
                            xmlGetAttr, "currency")
    
    df <- data.frame(
      rep_id = rep_id,
      customer = customer,
      country = country,
      date = date,
      product = product,
      qty = qty,
      total_amount = total_amount,
      currency = currency
    )
    df$date <- as.Date(df$date, "%m/%d/%Y")
  }
  return (df)
}

# convert xml to dataframes
reps_df <- data.frame()
reps_xml_path <- loadXMLfiles("txn-xml", "pharmaReps")
for (rep_xml_path in reps_xml_path){
  reps_df <- rbind(reps_df, writeXMLfiles(rep_xml_path, "//rep"))
}

sales_xml_df <- data.frame()
sales_xml_path <- loadXMLfiles("txn-xml", "pharmaSales")
for (sale_xml_path in sales_xml_path){
  sales_xml_df <- rbind(sales_xml_df, writeXMLfiles(sale_xml_path, "//txn"))
}

# create dataframes for each table
products_df <- data.frame(name = sales_xml_df$product)
products_df <- distinct(products_df, name)
products_df <- add_column(products_df, product_id = 1000 + 
                            seq(nrow(products_df)), .before = "name")

customers_df <- data.frame(name = sales_xml_df$customer,
                           country = sales_xml_df$country)
customers_df <- distinct(customers_df, name, country)
customers_df <- add_column(customers_df, customer_id = 100000 + 
                             seq(nrow(customers_df)), .before = "name")

num_sales <- nrow(sales_xml_df)
sales_df <- data.frame(sales_id = 200000 + seq(num_sales),
                       rep_id = sales_xml_df$rep_id,
                       customer_id = 1,
                       product_id = 1,
                       date = sales_xml_df$date,
                       qty = sales_xml_df$qty,
                       currency = sales_xml_df$currency,
                       total_amount = sales_xml_df$total_amount)

# Link tables using keys
for (i in 1:num_sales){
  cust_id <- customers_df$customer_id[which(
    customers_df$name == sales_xml_df$customer[i] & 
      customers_df$country == sales_xml_df$country[i])]
  sales_df$customer_id[i] <- cust_id
  
  prod_id <- products_df$product_id[which(
    products_df$name == sales_xml_df$product[i])]
  sales_df$product_id[i] <- prod_id
}

# Upload to cloud database server
dbWriteTable(db, "products", products_df, append = T, row.names = F)
dbWriteTable(db, "reps", reps_df, append = T, row.names = F)
dbWriteTable(db, "customers", customers_df, append = T, row.names = F)
dbWriteTable(db, "sales", sales_df, append = T, row.names = F)

# Close the database connection
dbDisconnect(db)

