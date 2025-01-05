# Load packages
pacman::p_load(RMySQL, DBI)

# Create a new SQLite database connection
# Database with star schema
db_star <- dbConnect(RMySQL::MySQL(),
                dbname = "sql5699558",
                host = "sql5.freemysqlhosting.net",
                user = "sql5699558",
                password = "Ip2dMxppa6")

# Database with normalized schema
db_norm <- dbConnect(RMySQL::MySQL(),
                dbname = "sql5698177",
                host = "sql5.freemysqlhosting.net",
                user = "sql5698177",
                password = "MuHk4ATCUX")


dbExecute(db1, "DROP TABLE IF EXISTS product_facts")
dbExecute(db1, "DROP TABLE IF EXISTS rep_facts")


# Create product fact table
prod_facts_df <- dbGetQuery(db_norm, "
SELECT 
    s.sales_id,
    p.product_id,
    c.customer_id,
    s.rep_id,
    p.name,
    c.country,
    SUM(s.total_amount) AS total_amount_sold,
    YEAR(s.date) AS year,
    QUARTER(s.date) AS quarter,
    COUNT(p.product_id) AS units_sold
FROM 
    sales s
JOIN 
    products p ON s.product_id = p.product_id
JOIN
    customers c ON c.customer_id = s.customer_id
GROUP BY
    p.name, year, quarter, c.country;
")

# Load data into MySQL server
dbWriteTable(db_star, "product_facts", prod_facts_df, append = T, row.names = F)

# Create rep fact table
rep_facts_df <- dbGetQuery(db_norm, "
SELECT
    s.sales_id,
    r.rep_id,
    CONCAT(r.first_name, ' ', r.sur_name) AS name,
    r.territory,
    SUM(s.total_amount) AS total_sold,
    AVG(s.total_amount) AS average_sold,
    YEAR(s.date) AS year,
    QUARTER(s.date) AS quarter
FROM
    sales s
JOIN
    reps r ON r.rep_id = s.rep_id
GROUP BY
    name, year, quarter;
")

# Load data into MySQL server
dbWriteTable(db_star, "rep_facts", rep_facts_df, append = T, row.names = F)

# Close database connections
dbDisconnect(db_star, db_norm)

