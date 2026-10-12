# Databricks notebook source
# MAGIC %run ./Common-Setup-Script

# COMMAND ----------

# Write your custom Python/PySpark code here

# COMMAND ----------

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Write your custom DBSQL code here

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 🗑️ Drop the user-created `CONFIG['abac_catalog_name']` catalog and all its contents.
# MAGIC DROP CATALOG IF EXISTS IDENTIFIER(CONFIG['abac_catalog_name']) CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -