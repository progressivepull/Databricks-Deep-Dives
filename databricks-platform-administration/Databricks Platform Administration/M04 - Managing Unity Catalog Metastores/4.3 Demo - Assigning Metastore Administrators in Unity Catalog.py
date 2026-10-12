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
# MAGIC # Demo - Assigning Metastore Administrators in Unity Catalog
# MAGIC
# MAGIC ## Overview
# MAGIC In this demo, we will learn how to assign Metastore Administrators in Unity Catalog to streamline data governance processes. We will walk through practical steps including accessing the Account Console, identifying the current metastore admin, and transferring administrative rights to a designated group. By the end of this demo, you will be able to delegate metastore administrative tasks effectively, ensuring efficient and scalable management of your data ecosystem.
# MAGIC
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this demo, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Identify the responsibilities and roles of a Metastore Administrator in Unity Catalog</li>
# MAGIC         <li>Demonstrate how to log into the Account Console and access the targeted metastore</li>
# MAGIC         <li>Identify the current metastore admin and determine when to transfer administrative rights</li>
# MAGIC         <li>Perform the process of assigning a new group as the metastore administrator</li>
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
# MAGIC * **Stage 1:** Assigning a Group as the Metastore Administrator
# MAGIC
# MAGIC
# MAGIC #### Note: 
# MAGIC   * These instructions should only be performed in an environment where you have been granted admin access.
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Understanding Metastore Administration
# MAGIC By default, the account administrator who created the metastore is the administrator for the metastore. 
# MAGIC
# MAGIC Metastore admin responsibilities are numerous and include:
# MAGIC * Creating and assigning permissions on catalogs,
# MAGIC * Creating and removing databases, tables, and views if other users aren't permitted to do so under your data governance policies, 
# MAGIC * Integrating external storage into the metastore, 
# MAGIC * Setting up shares with delta sharing, 
# MAGIC * And general maintenance and administrative tasks on data objects when their users are not accessible.
# MAGIC   * For example, transferring ownership of a schema owned by a user who has left the organization.
# MAGIC
# MAGIC Account admins are very likely already busy individuals. So, to avoid bottlenecks in your data governance processes, it's important to grant metastore admin to a group so that anyone in the group can take on these tasks.
# MAGIC
# MAGIC As roles change, users can be easily added or removed from the group.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Assigning a Group as the Metastore Administrator
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **Catalog**. This will display a list of currently existing **Metastores**.
# MAGIC 3. Locate the target metastore using the search field if required. \(**For Example:** In this case, it is **main-us-east** metastore.\)
# MAGIC 4. Select the target metastore **main-us-east** to access its details page.
# MAGIC 5. Ensure that you are under the **Configuration** tab.
# MAGIC 6. Locate the **Metastore Admin** field.
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * Currently, this happens to be the individual in the organization who originally created the metastore.
# MAGIC   * We need to change it to an administrator group. \(**For Example:** In this case, it is  **metastore_admins_test** group.\)
# MAGIC
# MAGIC 7. Select the **Edit** option right next to the currently assigned administrator. It will open a configuration prompt to change this assignment.
# MAGIC 8. Use the search field that also functions as a drop-down to find users, service principals, or other groups that want to assign as the new workspace administrator. In this case search for the **metastore_admins_test** group and select it.
# MAGIC 9. Review the prompt and then select the **Save** option to confirm the new metastore administrator assignment.
# MAGIC 10. Confirm that the new group **metastore_admins_test** appears as the new **metastore administrator** in the **Metastore Admin** field.
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * Ensure that you have account admin privileges before beginning this process.
# MAGIC   * Be mindful of the current admin’s role and ensure alignment with governance policies before transferring the ownership to a new admin.
# MAGIC   * Verify that the newly chosen metastore administrator has the correct access level and meets the organization’s security policies.
# MAGIC   * This action grants the group administrative capabilities, enabling them to manage metastore-related tasks efficiently.
# MAGIC   * Ensure that group members can now perform tasks related to metastore administration.
# MAGIC   * Test the changes by having a member of the new admin group verify their access and perform a sample task.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Conclusion
# MAGIC
# MAGIC You have successfully learned how to assign and manage metastore administrators who oversee Unity Catalog configuration and governance. This demo illustrated how metastore admin privileges control who can create catalogs, manage schemas and tables, and configure tags and policies within a metastore.
# MAGIC
# MAGIC Key takeaways from this demo include:
# MAGIC
# MAGIC - **Metastore administrators** hold broad control over Unity Catalog objects and should be carefully selected and audited.  
# MAGIC - **Assigning metastore admins** grants capabilities such as creating catalogs, managing schemas, and configuring governance features.  
# MAGIC - **Removing metastore admins** reduces risk when responsibilities change or when elevated access is no longer required.  
# MAGIC - **Separation of duties** between account admins, metastore admins, and workspace admins supports stronger governance.  
# MAGIC - **Clear ownership** at the metastore level improves accountability for policy design, catalog organization, and data stewardship.  
# MAGIC
# MAGIC With hands-on experience assigning metastore administrators, you can now define clear governance roles and responsibilities that keep Unity Catalog both powerful and controlled.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>