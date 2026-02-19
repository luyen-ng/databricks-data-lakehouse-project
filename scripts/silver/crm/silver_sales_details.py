# Databricks notebook source
# Initialisation
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DateType
from pyspark.sql.functions import trim, col

# COMMAND ----------

# Read Bronze table
df = spark.table("workspace.bronze.crm_sales_details")

# COMMAND ----------

# ========== Transformations ==========
# Trimming
for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

# Cleaning date
df = (df.withColumn("sls_order_dt",
                   F.when((df.sls_order_dt == 0) | (F.length(df.sls_order_dt) != 8), None)
                   .otherwise(F.to_date(df.sls_order_dt.cast("string"), "yyyyMMdd"))
                   )
        .withColumn("sls_ship_dt",
                   F.when((df.sls_ship_dt == 0) | (F.length(df.sls_ship_dt) != 8), None)
                   .otherwise(F.to_date(df.sls_ship_dt.cast("string"), "yyyyMMdd"))
                    )
        .withColumn("sls_due_dt",
                   F.when((df.sls_due_dt == 0) | (F.length(df.sls_due_dt) != 8), None)
                   .otherwise(F.to_date(df.sls_due_dt.cast("string"), "yyyyMMdd"))
                    )
)   

# COMMAND ----------

# Sales and price corrections
df = (df.withColumn("sls_sales",
                   F.when(df.sls_sales.isNull() | 
                          (df.sls_sales <= 0) | 
                          (df.sls_sales != (df.sls_quantity * F.abs(df.sls_price))),
                          df.sls_quantity * F.abs(df.sls_price))
                   .otherwise(df.sls_sales)
                   )
        .withColumn("sls_price",
                   F.when(df.sls_price.isNull() | 
                          (df.sls_price <= 0), 
                          F.when(df.sls_quantity != 0, F.col("sls_sales") / df.sls_quantity)
                          .otherwise(None)
                          )
                   .otherwise(df.sls_price)
                   )
       )

# COMMAND ----------

# Rename columns
RENAME_MAP = {
    "sls_ord_num": "order_number",
    "sls_prd_key": "product_number",
    "sls_cust_id": "customer_id",
    "sls_order_dt": "order_date",
    "sls_ship_dt": "ship_date",
    "sls_due_dt": "due_date",
    "sls_sales": "sales_amount",
    "sls_quantity": "quantity",
    "sls_price": "price"
}
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)

# COMMAND ----------

# Sanity checks of dataframe
df.limit(10).display()
# df.limit(10).show()

# COMMAND ----------

# ========== Write to Silver table ==========
df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.crm_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sanity checks of Silver table
# MAGIC SELECT * FROM workspace.silver.crm_sales LIMIT 10