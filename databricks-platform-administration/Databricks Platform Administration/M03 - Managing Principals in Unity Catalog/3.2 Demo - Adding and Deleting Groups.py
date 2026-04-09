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
# MAGIC # Demo - Adding and Deleting Groups
# MAGIC
# MAGIC ## Overview
# MAGIC In this demo, we will learn how to manage groups and permissions within the Databricks environment. We’ll analyze the process of adding new groups, assigning members, and removing individuals from groups. Additionally, we will explore how group memberships automatically inherit privileges and how to delete groups in the Unity Catalog. By the end of this demo, you will have the necessary skills to effectively organize and manage user access, ensuring an efficient and secure data governance framework in Databricks.
# MAGIC
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this demo, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Explain the role of groups in simplifying user and permission management within a security model</li>
# MAGIC         <li>Apply the steps to create and name new groups in the Databricks Account Console</li>
# MAGIC         <li>Demonstrate how to add and remove members from a group using the search and selection functions</li>
# MAGIC         <li>Evaluate the benefits of group-based permissions versus individual-based permissions for data governance</li>
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
# MAGIC * **Stage 1:** Adding New Groups
# MAGIC * **Stage 2:** Adding Members to a Group
# MAGIC * **Stage 3:** Removing Members from a Group
# MAGIC * **Stage 4:** Deleting Groups
# MAGIC
# MAGIC
# MAGIC #### Note: 
# MAGIC   * These instructions should only be performed in an environment where you have been granted admin access.
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Identities in Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ### A1. Identities: Groups
# MAGIC Groups gather individual users and service principles into logical units to simplify management.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A2. Identities: Nesting Groups
# MAGIC Groups can also be nested within other groups if needed. Any grants to the group are automatically inherited by all members of the group.
# MAGIC
# MAGIC Data governance policies are generally defined in terms of roles, and groups provide a user management construct that nicely maps to such roles, simplifying the implementation of these governance policies. 
# MAGIC
# MAGIC In this way, permissions can be granted to groups in accordance with your organization's security policies, and users can be added to groups as per the rules within the organization.
# MAGIC
# MAGIC When users transition between roles, it's simple to move a user from one group to another. 
# MAGIC
# MAGIC Performing an equivalent operation when permissions are hardwired at the individual user level is significantly more intensive. 
# MAGIC
# MAGIC Likewise, as your governance model evolves and role definitions change, it's much easier to affect those changes in groups rather than having to replicate changes across a number of individual users.
# MAGIC
# MAGIC For these reasons, we advise implementing groups and granting data permissions to groups rather than individual users or service principles.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Adding New Groups
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **User Management**.
# MAGIC 3. Select the **Groups** tab.
# MAGIC 4. Select the **Add Group** option.
# MAGIC 5. Provide a name for the group \(For Example, in this case enter **analysts_test**\).
# MAGIC 6. Select the **Save** option.
# MAGIC 7. Repeat the **Steps 1-6** to create another group. \(For Example, in this case create **metastore_admins_test**\).
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * The group creation does not automatically add members; they must be added manually.
# MAGIC   * After creating the group, you can immediately add members or return to this task later.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Adding Members to a Group
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **User Management**.
# MAGIC 3. Select the **Groups** tab.
# MAGIC 4. In the **Groups** tab, locate the **analyst_test** group using the search field, if required.
# MAGIC 5. Select the custom group **analyst_test** to access its details.
# MAGIC 6. Ensure that you are under the **Members** tab.
# MAGIC 7. Select the **Add Members** option at the right corner of the screen below the **Members** tab.
# MAGIC 8. Use the search field that also functions as a drop-down to find users, service principals, or other groups that want to add. In this case search for the **Test User** and select it.
# MAGIC 9. Select the **Add** option to include the selected member into the **analyst_test** group.
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * Multiple principals can be added to a group simultaneously if necessary.
# MAGIC   * Adding members is immediate and affects permissions instantly.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Removing Members from a Group
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **User Management**.
# MAGIC 3. Select the **Groups** tab.
# MAGIC 4. In the **Groups** tab, locate the **analyst_test** group using the search field, if required.
# MAGIC 5. Select the custom group **analyst_test** to access its details.
# MAGIC 6. Ensure that you are under the **Members** tab.
# MAGIC 7. Use the search field that also functions as a drop-down to find users, service principals, or other groups that want to manage. In this case search for the **Test User**.
# MAGIC 8. Select the **kebab menu** at the far right of this member and select the **Remove** option.
# MAGIC 9. You'll be prompted to confirm. Review the prompt and then, select **Cancel** to keep that group member intact for the upcoming demos. **Confirming remove** will remove that member from the **analyst_test** group.
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * In a group the membership updates are instant, including adding or removing principals.
# MAGIC   * Removing principals only impacts group-based permissions, not permissions directly assigned to individuals.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Group Privileges and Best Practices
# MAGIC
# MAGIC **Inheritance of Permissions:** All members of a group, including child groups, inherit permissions granted to the parent group.
# MAGIC
# MAGIC **Best Practice:** Assigning privileges through groups is recommended to greatly simplify implementation and maintenance of an organization's security model.
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * This hierarchical privilege assignment is ideal for scalable and evolving security models.
# MAGIC
# MAGIC **Purpose of Group Deletion**
# MAGIC * As governance evolves, removing outdated groups may be necessary.
# MAGIC * Deleting a group only removes its structure and permissions, without affecting individual member accounts, nor will it affect the permissions granted directly to those individuals or child groups.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Deleting Groups
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **User Management**.
# MAGIC 3. Select the **Groups** tab.
# MAGIC 4. In the **Groups** tab, locate the **analyst_test** group using the search field, if required.
# MAGIC 5. Select the **kebab menu** at the far right of this member and select the **Delete** option.
# MAGIC 6. You'll be prompted to confirm. Review the prompt and then, select **Cancel** to keep the group intact for the upcoming demos. **Confirming delete** will remove the **analyst_test** group.
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * Deleting a group does not impact permissions assigned directly to individuals or sub-groups.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Conclusion
# MAGIC
# MAGIC You have successfully learned how to create and delete groups in the Databricks account console to centralize access control for collections of users. This demo illustrated how groups act as reusable containers for permissions, simplifying security management across workspaces, compute, and data objects.
# MAGIC
# MAGIC Key takeaways from this demo include:
# MAGIC
# MAGIC - **Groups** bundle users into logical units (for example, teams, roles, or functions) for easier permission management.  
# MAGIC - **Creating groups** enables you to assign access once to the group instead of repeating grants for each individual user.  
# MAGIC - **Deleting groups** removes obsolete permission containers, reducing configuration drift and security risk.  
# MAGIC - **Nested or role-based groups** support scalable role design, separating “who someone is” from the specific resources they can access.  
# MAGIC - **Group-centric governance** is a best practice for managing access at scale across the Databricks Lakehouse.  
# MAGIC
# MAGIC With hands-on experience in adding and deleting groups, you can now structure role-based access patterns that make privilege assignments more consistent, auditable, and easy to maintain.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>