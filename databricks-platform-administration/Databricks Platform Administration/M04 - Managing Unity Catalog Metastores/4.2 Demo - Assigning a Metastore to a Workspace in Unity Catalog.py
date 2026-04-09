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
# MAGIC # Demo - Assigning a Metastore to a Workspace in Unity Catalog
# MAGIC
# MAGIC ## Overview
# MAGIC In this demo, we will learn how to assign a metastore to a workspace in the Unity Catalog. Through a series of guided steps, we'll cover the prerequisites for metastore assignments, how to locate and select a metastore, and the process of assigning it to the desired workspace. Additionally, we'll explore how to detach a metastore if needed and review best practices to ensure a smooth transition without disrupting active workloads. By the end of this demo, you will be prepared to manage metastore assignments effectively within the Databricks environment.
# MAGIC
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this demo, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Explain the role and structure of metastores within the Unity Catalog</li>
# MAGIC         <li>Illustrate the steps to create a new metastore with specified configurations</li>
# MAGIC         <li>Analyze the configuration details needed for metastore setup</li>
# MAGIC         <li>Evaluate the process of deleting a metastore while ensuring data integrity</li>
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
# MAGIC * **Stage 1:** Assigning a Metastore to a Workspace
# MAGIC * **Stage 2:** Reassigning or Detaching a Metastore
# MAGIC
# MAGIC
# MAGIC #### Note: 
# MAGIC   * These instructions should only be performed in an environment where you have been granted admin access.
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Working with Metastores
# MAGIC Metastore assignments can be done at any time, although if shuffling assignments, take care that no workloads are running that might be disturbed.
# MAGIC
# MAGIC These are some major key rules to remember while assigning, reassigning, or detaching a Metastore to a Workspace:
# MAGIC * A metastore must be in the same region as the workspace it is assigned to.
# MAGIC * A metastore can be assigned to multiple workspaces, provided they are all in the same region.
# MAGIC * A workspace can only have one active metastore at a time.
# MAGIC * Metastore assignments can occur at any time, but ensure no active workloads are disrupted when changing assignments.
# MAGIC
# MAGIC
# MAGIC **Note:**
# MAGIC * Switching metastores should be done with care to prevent interruptions to running workloads.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Assigning a Metastore to a Workspace
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **Catalog**. This will display a list of currently existing **Metastores**.
# MAGIC 3. Locate the target metastore using the search field if required.  \(**For Example:** In this case, it is **main-us-east** metastore.\)
# MAGIC 4. Select the target metastore **main-us-east** to access its details page.
# MAGIC 5. Select the **Workspaces** tab to review the current workspace assignments.
# MAGIC 6. Select the **Assign to workspace** option.
# MAGIC 7. Use the search field to locate the target workspace you want to assign the metastore to. In this case, search for **test-workspace** and check the box next to its name in the workspace list displayed below the search field.
# MAGIC 8. Once you have selected the workspace, select the **Assign** option.
# MAGIC 9. A prompt will appear asking you to enable the **Unity Catalog** functionality. Review the prompt and then select the **Enable** option to confirm.
# MAGIC
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * Once you finish the above steps, you will notice that the selected workspace is attached to the metastore.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Reassigning or Detaching a Metastore
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **Catalog**. This will display a list of currently existing **Metastores**.
# MAGIC 3. Locate the target metastore using the search field if required. \(**For Example:** In this case, it is **main-us-east** metastore.\)
# MAGIC 4. Select the target metastore **main-us-east** to access its details page.
# MAGIC 5. Select the **Workspaces** tab to review the current workspace assignments.
# MAGIC 6. Locate the target workspace using the search field if required. \(**For Example:** In this case, it is **test-workspace**.\)
# MAGIC 7. Select the **kebab menu** at the far right of this member and select the **Remove from this metastore** option.
# MAGIC 8. You'll be prompted to confirm. Review the prompt and then, select **Cancel** to keep this assignment intact for the upcoming demos. **Confirming unassign** will unassign the **main-us-east** metastore from the **test-workspace**.
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * Reassigning or detaching should be done when workloads can safely be paused or are not active.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Conclusion
# MAGIC
# MAGIC You have successfully learned how to assign a Unity Catalog metastore to a Databricks workspace so that the workspace can use centralized governance for catalogs, schemas, and tables. This demo showed how metastore assignment links workspaces to the appropriate governance boundary and what happens when those assignments change.
# MAGIC
# MAGIC Key takeaways from this demo include:
# MAGIC
# MAGIC - **Metastore-workspace assignment** enables Unity Catalog features (catalogs, schemas, tables, tags, and policies) in that workspace.  
# MAGIC - **Selecting the right metastore** ensures that a workspace participates in the correct regional and compliance domain.  
# MAGIC - **Reassigning or unassigning** a metastore affects how data is governed and should be coordinated with migration and change-management plans.  
# MAGIC - **Multi-workspace sharing** of a metastore supports consistent policies and data definitions across environments.  
# MAGIC - **Assignment operations** are restricted to account admins, reinforcing centralized control over governance boundaries.  
# MAGIC
# MAGIC With hands-on experience assigning metastores to workspaces, you can now connect your governance topology to actual environments, enabling Unity Catalog-based security and discovery where it is needed.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>