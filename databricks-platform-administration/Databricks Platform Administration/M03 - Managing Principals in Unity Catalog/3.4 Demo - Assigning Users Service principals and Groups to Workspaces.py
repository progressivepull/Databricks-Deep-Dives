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
# MAGIC # Demo - Assigning Users, Service principals, and Groups to Workspaces
# MAGIC
# MAGIC ## Overview
# MAGIC In this demo, we will learn how to assign and manage users, service principals, and groups within Databricks workspaces. We will cover the steps to assign these principals with specific permissions, such as regular user or workspace administrator, and how to remove them when necessary. By the end of this demo, you will be able to efficiently manage access to workspaces, ensuring proper permissions and control over who can access and administer workspace resources.
# MAGIC
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this demo, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Identify the steps to assign users, service principals, and groups to workspaces</li>
# MAGIC         <li>Explain the process of managing workspace permissions</li>
# MAGIC         <li>Assign a user to a workspace with specified permissions</li>
# MAGIC         <li>Determine when to remove a principal from a workspace and how to confirm the action</li>
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
# MAGIC * **Stage 1:** Assigning Users, Service Principals, and Groups to Workspaces
# MAGIC * **Stage 2:** Unassigning Users, Service Principals, or Groups from Workspaces
# MAGIC
# MAGIC
# MAGIC #### Note: 
# MAGIC   * These instructions should only be performed in an environment where you have been granted admin access.
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Assigning Users, Service Principals, and Groups to Workspaces
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **Workspace**.
# MAGIC 3. Locate the desired workspace using the search field, if required. \(For Example, in this case enter **test-workspace**\).
# MAGIC 4. Select the **test-workspace** to access its details page.
# MAGIC 5. Select the **Permissions** tab.
# MAGIC 6. Select the **Add permissions** option.
# MAGIC 7. Use the search field that also functions as a drop-down to find users, service principals, or groups that want to assign to this workspace. In this case search for the **Test User** and select it.
# MAGIC 8. In the **Permission** dropdown, select whether to add the selected user, service principal, or group as a regular **User** or as a **Workspace Administrator**. In this case, set the permission for the **Test User** as **User**.
# MAGIC 9. After specifying the principal and permissions, select the **Save** option to assign the **Test User** to the workspace.
# MAGIC
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * The Workspaces section allows you to manage all your workspaces and their permissions.
# MAGIC   * The Permissions tab allows you to manage access rights and assign different principals to the workspace.
# MAGIC   * The permissions you choose will determine the level of access the principal has within the workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Unassigning Users, Service Principals, or Groups from Workspaces
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **Workspace**.
# MAGIC 3. Locate the desired workspace using the search field, if required. \(For Example, in this case enter **test-workspace**\).
# MAGIC 4. Select the **test-workspace** to access its details page.
# MAGIC 5. Select the **Permissions** tab.
# MAGIC 6. Use the search field that also functions as a drop-down to find users, service principals, or other groups that want to manage. In this case search for the **Test User**.
# MAGIC 7. Select the **kebab menu** at the far right of this member and select the **Remove** option.
# MAGIC 8. You'll be prompted to confirm. Review the prompt and then, select **Cancel** to keep that workspace member intact for the upcoming demos. **Confirming remove** will remove that member from the **test-workspace**.
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * Always ensure you want to remove a user before confirming.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Conclusion
# MAGIC
# MAGIC You have successfully learned how to assign and unassign users, service principals, and groups to Databricks workspaces from the account console. This demo showed how workspace assignment bridges account-level identities with specific environments, ensuring that only the right principals can log in and use workspace resources.
# MAGIC
# MAGIC Key takeaways from this demo include:
# MAGIC
# MAGIC - **Workspace assignments** determine which users, groups, and service principals can access a given Databricks workspace.  
# MAGIC - **Assigning identities** to a workspace enables authentication and interaction with notebooks, jobs, clusters, and data in that environment.  
# MAGIC - **Removing assignments** cleanly cuts off workspace access for identities that no longer require it, supporting least privilege.  
# MAGIC - **Group-based assignment** scales better than assigning individual users, especially for large or dynamic teams.  
# MAGIC - **Alignment with account governance** ensures that identity, workspace, and data governance policies all work together consistently.  
# MAGIC
# MAGIC With hands-on experience assigning users, groups, and service principals to workspaces, you can now connect account-level identities to the right environments, forming the access layer on which compute and data governance are built.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>