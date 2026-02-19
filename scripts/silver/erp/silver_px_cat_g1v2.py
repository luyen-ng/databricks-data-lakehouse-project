# Databricks notebook source
# Initialisation
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col

# COMMAND ----------

# Read Bronze table
df = spark.table("workspace.bronze.erp_px_cat_g1v2")

# COMMAND ----------

# ========== Transformations ==========
# Trimming
for field in df.schema.fields:
    if isinstance(field.name, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

# Normalise Maintenance Flag to Boolean
df = df.withColumn("maintenance",
                   F.when(F.upper(col("maintenance")) == "YES", F.lit(True))
                   .when(F.upper(col("maintenance")) == "NO", F.lit(False))
                   .otherwise(None)
                   )

# COMMAND ----------

# Rename Columns
RENAME_MAP = {
    "id": "category_id",
    "cat": "category",
    "subcat": "subcategory",
    "maintenance": "maintenance_flag"
}
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)

# COMMAND ----------

# Sanity checks of dataframe
df.limit(10).display()

# COMMAND ----------

# ========== Write to Silver table ==========
df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.erp_product_category")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sanity checks of Silver table
# MAGIC SELECT * FROM workspace.silver.erp_product_category LIMIT 10