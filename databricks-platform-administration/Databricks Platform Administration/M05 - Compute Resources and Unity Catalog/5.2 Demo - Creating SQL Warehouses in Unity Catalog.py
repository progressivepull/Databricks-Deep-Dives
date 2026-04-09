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

# MAGIC %md-sandbox
# MAGIC # Demo - Creating SQL Warehouses in Unity Catalog
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this demo, we will learn how to create and configure a SQL Warehouse in the Databricks environment. We will walk through the steps of setting up a new SQL Warehouse, configuring its settings, and enabling Unity Catalog integration. Additionally, we will explore how to manage permissions and monitor the warehouse's status. By the end of this demo, you will have the necessary skills to efficiently create and manage SQL Warehouses in Databricks, enabling you to run SQL queries and integrate with external BI tools seamlessly.
# MAGIC
# MAGIC
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this demo, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Identify the steps to create a SQL Warehouse in Databricks</li>
# MAGIC         <li>Explain the function of SQL Warehouses in the Databricks environment</li>
# MAGIC         <li>Demonstrate how to set up a SQL Warehouse with specific configuration options in Databricks</li>
# MAGIC         <li>Monitor and interpret the status of a SQL Warehouse during and after creation</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #2196f3; background: #e3f2fd; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">📋</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #1565c0; font-size: 1.1em;">Prerequisites</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">In order to follow along with this demo, you will need:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li><strong>Account administrator</strong> capabilities.</li>
# MAGIC         <li>Cloud resources to support the <strong>metastore</strong>.</li>
# MAGIC         <li>Have <strong>metastore admin</strong> capability in order to create and manage a catalog</li>
# MAGIC         <li>Access to a <strong>Unity-Catalog enabled Databricks Workspace</strong> with the ability to create catalogs in your metastore.</li>
# MAGIC         <li>Unrestricted ability to <strong>create clusters</strong> in your workspace.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Tasks to Perform
# MAGIC
# MAGIC As part of this demo, you are required to perform the following tasks:
# MAGIC * **Stage 1:** Creating a SQL Warehouse
# MAGIC * **Stage 2:** Verifying the SQL Warehouse Status
# MAGIC
# MAGIC
# MAGIC #### Note: 
# MAGIC   * These instructions should only be performed in an environment where you have been granted admin access.
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Demo Overview
# MAGIC In this Demo, we see how to configure an SQL Warehouse cluster to access Unity Catalog.
# MAGIC
# MAGIC The same configuration principles can also be applied when configuring SQL Warehouse based job clusters for running automated workloads.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Creating a SQL Warehouse Compute in Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Steps to create a SQL Warehouse
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **Workspace**.
# MAGIC 3. Locate the desired workspace using the search field, if required. \(For Example, in this case enter **test-workspace**\).
# MAGIC 4. Select the **test-workspace** to access its details page.
# MAGIC 5. Ensure that you are under the **Configuration** tab.
# MAGIC 6. Select the **URL** for the workspace on the right side of the **Configuration** page. This will redirect you to that Databricks Workspace.
# MAGIC 7. In the **left sidebar** within the newly opened Databricks Workspace, select **Compute**.
# MAGIC 8. Select the **SQL warehouses** tab. It will list all the currently existing SQL Warehouses.
# MAGIC 9. Select the **Create SQL Warehouse** option. A **setup box** to configure your **New SQL Warehouse** instance will pop up on your screen.
# MAGIC 10. Fill in the SQL warehouse details in this setup box with relevant details as specified below:
# MAGIC | Field Name | Instructions |
# MAGIC |--|--|
# MAGIC | Name | Enter a unique name such as **test_workspace**. |
# MAGIC | Cluster size | Set this to **2X-Small** \(to reduce the cost in the lab environment\). |
# MAGIC | Auto stop | It has a **toggle button** to enable/disable this feature along with a field to **set the duration of activity** in minutes. <br/> **Enable this feature** and by default its duration should be set to **10 minutes** \(if not, set it to **10 minutes**\).  |
# MAGIC | Scaling | Set this to **1 cluster for the Minimum** and **1 cluster for the Maximum**. |
# MAGIC | Type | It has three options: **Serverless**, **Pro**, and **Classic**. <br/>Select the **Serverless** option. |
# MAGIC | Advanced options | This section contains some advanced settings. <br/>**Expand** this section. |
# MAGIC | Tags | This feature is a **collection of key-value pairs** that are automatically added to cluster instances for tracking usage in your cloud provider. <br/>**Do not change anything** for this feature. |
# MAGIC | Unity Catalog | This features has a toggle button to enable the **Unity Catalog** feature for the SQL Warehouse. <br/>It is **enabled by default** \(if not, enable it\). |
# MAGIC | Channel | It has two options: **Current**, and **Preview**. <br/>The **Current** option is selected by default \(if not, set this to the **Current** option\). |
# MAGIC
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * The **channel** feature is used to introduce new features and releases while minimizing disruption.
# MAGIC
# MAGIC 11. Once all information is entered, verify them and select the **Create** option.
# MAGIC 12. You’ll be prompted to **Manage permissions**. Review the prompt to add a new permission.
# MAGIC 13. In the **Manage Permissions** prompt, use the search field, which also acts as a drop-down, to find and select the **All Users** principal. Then, set the permission next to it to **Can Use**.
# MAGIC 14. Once all information is entered, verify them and close the **Manage Permissions** prompt to save the changes.
# MAGIC
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * This operation may take a few moments. During this time, you will be redirected to the **SQL Warehouse overview page**, where you can **monitor the current status** of the SQL Warehouse creation.
# MAGIC   * **Status:**
# MAGIC     * Initially, the status will be **Starting** during the launch process.
# MAGIC     * Once the SQL Warehouse is online, the status will change to **Running**.
# MAGIC     * When the warehouse is stopped, the status will be updated to **Stopped**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Verifying the SQL Warehouse Status
# MAGIC 1. Once the status changes to **Running**, navigate to the **left sidebar** in Databricks Workspace and select **SQL Warehouses** under the **SQL** section.
# MAGIC 2. Locate the **test_workspace** that we just created using the search field if required.
# MAGIC 3. Verify that the **test_workspace** appears in the list and its status is showing as **running**.
# MAGIC 4. This **confirms** that you have **successfully created a SQL Warehouse** within the Unity Catalog in Databricks.
# MAGIC
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * Now that the compute resources are created, you can begin running notebooks and queries in the Databricks workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Conclusion
# MAGIC
# MAGIC You have successfully learned how to create Unity Catalog-enabled SQL warehouses that provide governed, scalable compute for BI and SQL workloads. This demo showed how to configure a warehouse with the correct Unity Catalog settings, size, and auto-stop behavior to balance performance, cost, and security.
# MAGIC
# MAGIC Key takeaways from this demo include:
# MAGIC
# MAGIC - **SQL warehouses** provide dedicated, scalable compute for SQL queries, dashboards, and BI tools.  
# MAGIC - **Unity Catalog-enabled warehouses** enforce catalog, schema, and table permissions consistently for all connected users.  
# MAGIC - **Configuring warehouse size and scaling** allows you to match performance and cost to workload patterns.  
# MAGIC - **Auto-stop and scheduling options** help control spend while keeping critical workloads responsive.  
# MAGIC - **Warehouses as shared compute endpoints** make it easier to standardize governance and performance for analytics users.  
# MAGIC
# MAGIC With hands-on experience creating Unity Catalog–aware SQL warehouses, you can now provision governed SQL endpoints that deliver secure, performant access to Lakehouse data for analysts and BI consumers.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>