# Databricks notebook source
# Transformation Logic
query = """
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.start_date, pn.product_number) AS product_key,
    pn.product_id,
    pn.product_number,
    pn.product_name,
    pn.category_id,
    pc.category,
    pc.subcategory,
    pc.maintenance_flag,
    pn.product_line,
    pn.start_date
FROM silver.crm_products pn
LEFT JOIN silver.erp_product_category pc
    ON pn.category_id = pc.category_id
WHERE pn.end_date IS NULL; -- Filter out all historical data
"""
df = spark.sql(query)

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# Write into Gold Table
df.write.mode("overwrite").format("delta").saveAsTable("workspace.gold.dim_products")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sanity checks of Gold table
# MAGIC SELECT * FROM workspace.gold.dim_products LIMIT 10