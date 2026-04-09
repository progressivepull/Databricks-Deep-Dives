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
# MAGIC # Demo - Adding and Deleting Service Principals
# MAGIC
# MAGIC ## Overview
# MAGIC In this demo, we will learn how to manage service principals within the Databricks environment. We'll walk through the steps to add a new service principal, provide an appropriate name for identification, and explore the deletion process. By the end of this demo, you will be equipped with the skills to efficiently add and delete service principals, helping you manage service access and security within the Databricks workspace.
# MAGIC
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this demo, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Describe the purpose of assigning a name to a service principal during its creation</li>
# MAGIC         <li>Demonstrate how to add a new service principal in the Unity Catalog</li>
# MAGIC         <li>Identify the steps required to delete a service principal in the Unity Catalog</li>
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
# MAGIC * **Stage 1:** Adding a Service Principal
# MAGIC * **Stage 2:** Deleting a Service Principal
# MAGIC
# MAGIC
# MAGIC #### Note: 
# MAGIC   * These instructions should only be performed in an environment where you have been granted admin access.
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Adding a Service Principal
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **User Management**.
# MAGIC 3. Select the **Service principals** tab.
# MAGIC 4. Select the **Add service principal** option.
# MAGIC 5. Provide a name for the service principal \(For Example, in this case enter **terraform_test**\).
# MAGIC 6. Select the **Add** option.
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * The name **terraform_test** is for administrative purposes only and does not affect system identification.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Deleting a Service Principal
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **User Management**.
# MAGIC 3. Select the **Service principals** tab.
# MAGIC 4. Locate the **terraform_test** service principal using the search field, if required.
# MAGIC 5. Select the service principal **terraform_test** to access its details page.
# MAGIC 6. Select the **kebab menu** at the top right corner of the page and select the **Delete** option.
# MAGIC 7. You'll be prompted to confirm. Review the prompt and then, select **Cancel** to keep the service principal intact for the upcoming demos. **Confirming delete** will remove that service principal.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Conclusion
# MAGIC
# MAGIC You have successfully learned how to add and manage service principals in the Databricks account console to represent non-human identities such as automation tools and external applications. This demo walked through creating a named service principal and reviewing the deletion flow, helping you understand how to control application-level access to your workspaces.
# MAGIC
# MAGIC Key takeaways from this demo include:
# MAGIC
# MAGIC - **Service principals** model machine or application identities that interact with Databricks APIs and resources.
# MAGIC - **Naming service principals** clearly (for example, <code>terraform_test</code>) improves traceability and operational clarity without affecting technical behavior.
# MAGIC - **Adding a service principal** from the Account Console under **User Management → Service principals** establishes the identity for downstream access and automation.
# MAGIC - **Deleting a service principal** removes its ability to authenticate and should be done carefully to avoid breaking active integrations.
# MAGIC - **Admin-only operations** on service principals reinforce that application access is a controlled, auditable part of workspace security.  
# MAGIC
# MAGIC With hands-on experience in service principal creation and deletion, you can now manage application access in a controlled manner, supporting automation scenarios while maintaining strong governance over who and what can interact with your Databricks Lakehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>