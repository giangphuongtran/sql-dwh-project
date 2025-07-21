# Data Catalog for Gold Layer

## Overview
The Gold Layer is the business-level data representation, structured to support analytical and reporting use cases. It consists of **dimension tables** and **fact tables** for specific business metrics.

---

### 1. **gold.dim_customers**
- **Purpose:** Stores customer details enriched with demographic and geographic data.
- **Columns:**

| Column Name       | Data Type | Description                                  | Source Table            | Notes                     |
|-------------------|-----------|----------------------------------------------|--------------------------|----------------------------|
| customer_id       | INT       | Internal customer ID                         | crm_cust_info            | 🔑 Primary Key             |
| customer_key      | TEXT      | Source system customer key                   | crm_cust_info            | Unique per source          |
| first_name        | TEXT      | First name of the customer                   | crm_cust_info            |                            |
| last_name         | TEXT      | Last name of the customer                    | crm_cust_info            |                            |
| marital_status    | TEXT      | Marital status                               | crm_cust_info            |                            |
| gender            | TEXT      | Gender (from CRM or ERP)                     | crm_cust_info + erp_cust_info |                     |
| country           | TEXT      | Customer's country of residence              | erp_loc_info             | Nullable                   |
| birth_date        | DATE      | Date of birth                                | erp_cust_info            | Nullable                   |


---

### 2. **gold.dim_products**
- **Purpose:** Provides information about the products and their attributes.
- **Columns:**

| Column Name         | Data Type | Description                                | Source Table            | Notes                     |
|---------------------|-----------|--------------------------------------------|--------------------------|----------------------------|
| product_id          | INT       | Internal product ID                        | crm_prd_info             | 🔑 Primary Key             |
| product_key         | TEXT      | Source system product key                  | crm_prd_info             | Unique per source          |
| product_name        | TEXT      | Name of the product                        | crm_prd_info             |                            |
| product_cost        | NUMERIC   | Cost of the product                        | crm_prd_info             |                            |
| product_line        | TEXT      | Product line or brand category             | crm_prd_info             |                            |
| category_id         | INT       | ID of the product category                 | crm_prd_info             |                            |
| category_name       | TEXT      | Category name                              | erp_px_cat               |                            |
| sub_category_name   | TEXT      | Subcategory name                           | erp_px_cat               |                            |
| maintenance         | TEXT      | Maintenance flag or note                   | erp_px_cat               |Yes/No                      |
| product_start_date  | DATE      | Start date of the product                  | crm_prd_info             |                            |

---

### 3. **gold.fact_sales**
- **Purpose:** Stores transactional sales data for analytical purposes.
- **Columns:**

| Column Name       | Data Type | Description                                | Source Table            | Notes                     |
|-------------------|-----------|--------------------------------------------|--------------------------|----------------------------|
| order_number      | TEXT      | Sales order number                         | crm_sales_details        | 🔑 Primary Key             |
| product_key       | TEXT      | Foreign key to dim_product                 | crm_sales_details        | 🔗 FK to dim_product       |
| customer_id       | INT       | Foreign key to dim_customer                | crm_sales_details        | 🔗 FK to dim_customer      |
| order_date        | DATE      | Order creation date                        | crm_sales_details        |                            |
| ship_date         | DATE      | Date when order was shipped                | crm_sales_details        |                            |
| due_date          | DATE      | Payment due date                           | crm_sales_details        |                            |
| sales_amount      | NUMERIC   | Total sale value                           | crm_sales_details        |                            |
| quantity          | INT       | Number of units sold                       | crm_sales_details        |                            |
| price             | NUMERIC   | Unit price of product                      | crm_sales_details        |                            |
