-- Fast lookup by customers
CREATE UNIQUE INDEX idx_dim_customer_id ON gold.dim_customer(customer_id);
CREATE UNIQUE INDEX idx_dim_customer_key ON gold.dim_customer(customer_key);

-- Fast lookup by products
CREATE UNIQUE INDEX idx_dim_product_key ON gold.dim_product(product_key);
CREATE UNIQUE INDEX idx_dim_product_id ON gold.dim_product(product_id);

-- Useful if category filtering is common
CREATE INDEX idx_dim_product_category ON gold.dim_product(category_id);

-- Foreign key-style joins
CREATE INDEX idx_fact_sales_customer_id ON gold.fact_sales(customer_id);
CREATE INDEX idx_fact_sales_product_key ON gold.fact_sales(product_key);

-- One client can have many orders, one order can have many products
CREATE UNIQUE INDEX idx_fact_order_number ON gold.fact_sales(order_number, customer_id, product_key);