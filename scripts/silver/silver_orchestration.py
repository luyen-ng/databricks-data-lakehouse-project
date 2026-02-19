# Databricks notebook source
notebooks = [
    "./crm/silver_cust_info",
    "./crm/silver_prd_info",
    "./crm/silver_sales_details",
    "./erp/silver_cust_az12",
    "./erp/silver_loc_a101",
    "./erp/silver_px_cat_g1v2"
]

for nb in notebooks:
    print(f"Running {nb}")
    dbutils.notebook.run(nb, timeout_seconds=0)