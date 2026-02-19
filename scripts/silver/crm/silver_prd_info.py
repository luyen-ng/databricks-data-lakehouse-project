# Databricks notebook source
# Initialisation
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DateType
from pyspark.sql.functions import trim, col
from pyspark.sql.window import Window

# COMMAND ----------

# Read Bronze table
df = spark.table("workspace.bronze.crm_prd_info")

# COMMAND ----------

# ========== Transformations ==========
# Trimming
for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

# Product Key Parsing
# Extract category ID
df = df.withColumn("cat_id", F.regexp_replace(F.substring(df.prd_key, 1, 5), "-", "_"))
# Extract product key
df = df.withColumn("prd_key", F.substring(df.prd_key, 7, F.length(df.prd_key)))

# COMMAND ----------

# Cost cleanup
df = df.withColumn("prd_cost", F.coalesce(df.prd_cost, F.lit(0)))

# COMMAND ----------

# Product line normalisation
df = df.withColumn("prd_line",
                   F.when(F.upper(df.prd_line) == "M", "Mountain")
                   .when(F.upper(df.prd_line) == "R", "Road")
                   .when(F.upper(df.prd_line) == "S", "Other Sales")
                   .when(F.upper(df.prd_line) == "T", "Touring")
                   .otherwise("n/a")
                   )

# COMMAND ----------

# Date Casting
df = df.withColumn("prd_start_dt", df.prd_start_dt.cast(DateType()))

# COMMAND ----------

# Renaming Columns
RENAME_MAP = {
    "prd_id": "product_id",
    "cat_id": "category_id",
    "prd_key": "product_number",
    "prd_nm": "product_name",
    "prd_cost": "product_cost",
    "prd_line": "product_line",
    "prd_start_dt": "start_date",
    "prd_end_dt": "end_date"
}
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)

# COMMAND ----------

# Sanity checks of dataframe
df.limit(10).display()

# COMMAND ----------

# ========== Write to Silver table ==========
df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.crm_products")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sanity checks of Silver table
# MAGIC SELECT * FROM workspace.silver.crm_products LIMIT 10