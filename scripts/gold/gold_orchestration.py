# Databricks notebook source
notebooks = [
    "./gold_dim_customers",
    "./gold_dim_products",
    "./gold_fact_sales"
]

for nb in notebooks:
    print(f"Running {nb}")
    dbutils.notebook.run(nb, timeout_seconds=0)