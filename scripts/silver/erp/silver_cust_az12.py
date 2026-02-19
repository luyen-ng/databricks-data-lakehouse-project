# Databricks notebook source
# Initialisation
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col

# COMMAND ----------

# Read Bronze table
df = spark.table("workspace.bronze.erp_cust_az12")

# COMMAND ----------

# ========== Transformations ==========
# Trimming
for field in df.schema.fields:
  if isinstance(field.dataType, StringType):
    df = df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

# Customer ID Cleanup
df = df.withColumn("cid",
                   F.when(col("cid").startswith("NAS"), 
                          F.substring(col("cid"), 4, F.length(col("cid"))))
                   .otherwise(col("cid"))
                   )

# COMMAND ----------

# Birthdate Validation
df = df.withColumn("bdate",
                   F.when(col("bdate") > F.current_date(), None)
                   .otherwise(col("bdate"))
                   )

# COMMAND ----------

# Gender Normalisation
df = df.withColumn("gen",
                   F.when(F.upper(col("gen")).isin("F", "FEMALE"), "Female")
                   .when(F.upper(col("gen")).isin("M", "MALE"), "Male")
                   .otherwise("n/a")
                    )

# COMMAND ----------

# Rename Columns
RENAME_MAP = {
    "cid": "customer_number",
    "bdate": "birth_date",
    "gen": "gender"
}
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)

# COMMAND ----------

# Sanity checks of dataframe
df.limit(10).display()

# COMMAND ----------

# ========== Write to Silver table ==========
df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.erp_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sanity checks of Silver table
# MAGIC SELECT * FROM workspace.silver.erp_customers LIMIT 10