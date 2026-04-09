# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Started with Databricks Platform Administration
# MAGIC
# MAGIC In this course, you will learn the basics of platform administration on the Databricks Data Intelligence Platform. It offers a comprehensive overview of the Unity Catalog, a vital component for effective data governance within Databricks environments. Divided into five modules, it begins with a detailed introduction to Databricks infrastructure and its data intelligence platform, including an in-depth walkthrough of the Databricks Workspace. You will explore data governance principles within Unity Catalog, covering its key concepts, architecture, and roles. 
# MAGIC The course further emphasizes managing Unity Catalog metastores and compute resources, including clusters and SQL warehouses. Finally, you'll master data access control by learning about privileges, fine-grained access, and how to govern data objects. 
# MAGIC
# MAGIC By the end, you will be equipped with essential skills to administer the Unity Catalog to implement effective data governance, optimize compute resources, and enforce robust data security strategies. With the purchase of a Databricks Labs subscription, the course also closes out with a comprehensive lab exercise to practice what you’ve learned in a live Databricks Workspace environment.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC The content was developed for participants with these skills/knowledge/abilities:  
# MAGIC - Basic knowledge of cloud computing and SQL concepts such as networking basics, SQL commands, aggregate functions, filters and sorting, indexes, tables, and views.
# MAGIC - Basic knowledge of Python programming, Jupyter notebook interface, and PySpark fundamentals.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Course Agenda
# MAGIC The following modules are part of the **Get Started with Databricks Platform Administration** course by **Databricks Academy**.
# MAGIC | Module Name &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Content &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|
# MAGIC | **Databricks Overview** | **Lecture -** Introduction and Overview <br> **Lecture -** Databricks Infrastructure <br> **Lecture -** Databricks Data Intelligence Platform <br> **Lecture -** Unity Catalog Overview <br> |
# MAGIC | **Data Governance in Unity Catalog** | **Lecture -** Data Governance Overview </br> **Lecture -** Unity Catalog Key Concepts <br> **Lecture -** Databricks Roles <br> **Lecture -** Databricks Identities <br> | 
# MAGIC | **[Managing Principles in Unity Catalog]($./M03 - Managing Principals in Unity Catalog)** | **Lecture -** Managing Principals in Unity Catalog - Overview </br> [Demo: Adding and Deleting Users]($./M03 - Managing Principals in Unity Catalog/3.1 Demo - Adding and Deleting Users) </br> [Demo: Adding and Deleting Groups]($./M03 - Managing Principals in Unity Catalog/3.2 Demo - Adding and Deleting Groups) </br> [Demo: Adding and Deleting Service Principals]($./M03 - Managing Principals in Unity Catalog/3.3 Demo - Adding and Deleting Service Principals) </br> [Demo: Assigning Users, Service Principals, and Groups to Workspaces]($./M03 - Managing Principals in Unity Catalog/3.4 Demo - Assigning Users Service principals and Groups to Workspaces) | 
# MAGIC | **[Managing Unity Catalog Metastores]($./M04 - Managing Unity Catalog Metastores)** | [Demo: Creating and Deleting Metastores in Unity Catalog]($./M04 - Managing Unity Catalog Metastores/4.1 Demo - Creating and Deleting Metastores in Unity Catalog) </br> [Demo: Assigning a Metastore to a Workspace in Unity Catalog]($./M04 - Managing Unity Catalog Metastores/4.2 Demo - Assigning a Metastore to a Workspace in Unity Catalog) </br> [Demo: Assigning Metastore Administrators in Unity Catalog]($./M04 - Managing Unity Catalog Metastores/4.3 Demo - Assigning Metastore Administrators in Unity Catalog)  | 
# MAGIC | **[Compute Resources and Unity Catalog]($./M05 - Compute Resources and Unity Catalog)** | **Lecture -** Clusters </br> [Demo: Creating a Cluster in Unity Catalog]($./M05 - Compute Resources and Unity Catalog/5.1 Demo - Creating a Cluster in Unity Catalog) </br> **Lecture -** SQL Warehouses </br> [Demo: Creating SQL Warehouses in Unity Catalog]($./M05 - Compute Resources and Unity Catalog/5.2 Demo - Creating SQL Warehouses in Unity Catalog) | 
# MAGIC | **[Data Access Control in Unity Catalog]($./M06 - Data Access Control in Unity Catalog)** | **Lecture -** Privileges in Unity Catalog </br> **Lecture -** Fine-grained Access Control </br> [Demo: Implementing Fine-Grained Access Control in Unity Catalog]($./M06 - Data Access Control in Unity Catalog/6.1 Demo - Implementing Fine-Grained Access Control in Unity Catalog)  | 
# MAGIC | **Comprehensive Experience** | [Lab: Implementing Fine-Grained Access Control for Global Financial Services]($./7.1 Lab - Implementing Fine-Grained Access Control for Global Financial Services)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run demo and lab notebooks, you need to use one of the following Databricks runtime(s): **`17.3.x-scala2.13`**

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>