# Databricks notebook source
# Transformation Logic
query = """
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.customer_id) AS customer_key,
    ci.customer_id,
    ci.customer_number,
    ci.first_name,
    ci.last_name,
    la.country,
    ci.marital_status,
    CASE
        WHEN ci.gender != "n/a" THEN ci.gender
        ELSE COALESCE(ca.gender, "n/a")
    END AS gender,
    ca.birth_date AS birthdate,
    ci.created_date AS create_date
FROM silver.crm_customers ci
LEFT JOIN silver.erp_customers ca
    ON ci.customer_number = ca.customer_number
LEFT JOIN silver.erp_customer_location la
    ON ci.customer_number = la.customer_number
"""
df = spark.sql(query)

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# Write into Gold table
df.write.mode("overwrite").format("delta").saveAsTable("workspace.gold.dim_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sanity checks of Gold table
# MAGIC SELECT * FROM workspace.gold.dim_customers LIMIT 10