# Databricks notebook source
# Initialisation
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col

# COMMAND ----------

# Read Bronze table
df = spark.table("workspace.bronze.crm_cust_info")

# COMMAND ----------

# ========== Transformations ==========
# Trimming
for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))

# Nomarlisation
df = (
    df
    .withColumn("cst_marital_status",
                   F.when(F.upper(F.col("cst_marital_status")) == "S", "Single")
                    .when(F.upper(F.col("cst_marital_status")) == "M", "Married")
                    .otherwise("n/a")
                )
    .withColumn("cst_gndr",
                   F.when(F.upper(F.col("cst_gndr")) == "M", "Male")
                    .when(F.upper(F.col("cst_gndr")) == "F", "Female")
                    .otherwise("n/a")
                )
)

# COMMAND ----------

# Remove records with missing customer ID
df = df.filter(df.cst_id.isNotNull())

# COMMAND ----------

# Renaming columns
RENAME_MAP = {
    "cst_id": "customer_id",
    "cst_key": "customer_number",
    "cst_firstname": "first_name",
    "cst_lastname": "last_name",
    "cst_marital_status": "marital_status",
    "cst_gndr": "gender",
    "cst_create_date": "created_date"
}
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)

# COMMAND ----------

# Sanity checks of dataframe
df.limit(10).display()

# COMMAND ----------

# ========== Write to Silver table ==========
df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.crm_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sanity checks of Silver table
# MAGIC SELECT * FROM workspace.silver.crm_customers LIMIT 10