# Databricks notebook source
# Initialisation
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col

# COMMAND ----------

# Read Bronze table
df = spark.table("workspace.bronze.erp_loc_a101")

# COMMAND ----------

# ========== Transformations ==========
# Trimming
for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

# Customer ID Cleanup
df = df.withColumn("cid", F.regexp_replace(col("cid"), "-", ""))

# COMMAND ----------

# Country Normalization
df = df.withColumn("cntry",
                   F.when(col("cntry") == "DE", "Germany")
                   .when(col("cntry").isin("US", "USA"), "United States")
                   .when((col("cntry") == "") | (col("cntry").isNull()), "n/a")
                   .otherwise(col("cntry"))
                   )

# COMMAND ----------

# Renaming Columns
RENAME_MAP = {
    "cid": "customer_number",
    "cntry": "country"
}
for old, new in RENAME_MAP.items():
    df = df.withColumnRenamed(old, new)

# COMMAND ----------

# Sanity checks of dataframe
df.limit(10).display()

# COMMAND ----------

# ========== Write to Silver table ==========
df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.erp_customer_location")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sanity checks of Silver table
# MAGIC SELECT * FROM workspace.silver.erp_customer_location LIMIT 10