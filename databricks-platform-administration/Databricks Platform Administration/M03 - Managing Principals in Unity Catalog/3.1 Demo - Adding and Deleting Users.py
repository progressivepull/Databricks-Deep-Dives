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
# MAGIC # Demo - Adding and Deleting Users
# MAGIC
# MAGIC ## Overview
# MAGIC In this demo, we will learn how to manage user accounts and access in the Databricks environment. Through a sequence of practical steps, we'll cover adding a new user, configuring user information, and validating user identities within Unity Catalog. We'll also explore how to assign privileges, create and manage catalogs, and finally, perform user deletions in a controlled manner. By the end of this demo, you will be equipped with the required skills to efficiently manage user access and data privileges within Databricks, ensuring streamlined and secure account administration.
# MAGIC
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this demo, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Identify steps to add a new user in the Databricks account console</li>
# MAGIC         <li>Describe the process of validating a user in Unity Catalog</li>
# MAGIC         <li>Demonstrate how to assign and manage privileges for a user</li>
# MAGIC         <li>Execute steps to delete a user and associated custom catalog in Databricks</li>
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
# MAGIC * **Stage 1:** Adding the New User
# MAGIC * **Stage 2:** Validating the New User by Creating a Custom Catalog
# MAGIC * **Stage 3:** Deleting the Custom Catalog
# MAGIC * **Stage 4:** Deleting a User
# MAGIC
# MAGIC #### Note: 
# MAGIC   * These instructions should only be performed in an environment where you have been granted admin access.
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Adding the New User
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **User Management**.
# MAGIC 3. Ensure that you are under the **User** tab.
# MAGIC 4. Select the **Add User** option.
# MAGIC 5. Fill in the user account details with relevant values for the new user as specified below:
# MAGIC | Field Name | Instructions |
# MAGIC |--|--|
# MAGIC | Email | Enter **db.analyst007@dispostable.com** |
# MAGIC | First Name | Enter **Test** |
# MAGIC | Last Name | Enter **User** |
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * This email is the unique identifier across the system.
# MAGIC   * It must be a valid email, since that will be used to confirm identity and manage their password. 
# MAGIC   * In this example, I'm using a temporary email address that I generated using dispostable.com.
# MAGIC   * Though these fields aren't strictly required by the system, they make identities more human-readable.
# MAGIC
# MAGIC 6. Select the **Add User** option.
# MAGIC
# MAGIC 7. Validate the **User Account** through the **Invitation Email**
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * The new user will be issued an email inviting them to join and set their password. 
# MAGIC   * Validate the User Account creation via the Confirmation Email sent to the registered email id.
# MAGIC   * The user is now part of the account but has no access to Databricks services yet since they are not yet assigned with a workspace access.
# MAGIC   * The user is recognized as a valid principal in Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Validating the New User by Creating a Custom Catalog
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **Workspace**.
# MAGIC 3. Locate the target workspace using the search field if required. \(**For Example:** In this case, it is **Curriculum Dev**\)
# MAGIC 4. Select the **workspace URL** on the right side to open the workspace in a New Tab.
# MAGIC 5. In the **left sidebar**, select **Catalog**.
# MAGIC 6. It will open the **Catalog Explorer** page where you will view the list of available catalogs in the **left catalog pane**.
# MAGIC 7. Select the **Create catalog** option.
# MAGIC 8. Fill in the catalog details with relevant details as specified below:
# MAGIC | Field Name | Instructions |
# MAGIC |--|--|
# MAGIC | Catalog_name | Enter **test_catalog** |
# MAGIC | Type | Select the **Standard** option from the drop-down list. |
# MAGIC | Storage location | Leave this field blank. |
# MAGIC | Comment | Provide any comments as per your preference/leave it blank. |
# MAGIC
# MAGIC 9. Select the **Create** option. The new catalog appears alongside other catalogs.
# MAGIC 10. Select the custom catalog and expand it to see the three-level namespace in action.
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * Avoid selecting/expanding the hive metastore, samples, or system catalogs.
# MAGIC   * Expanding components shows the namespace structure.
# MAGIC
# MAGIC 11. Within the custom catalog on the right section of your screen, select the **Permissions** tab.
# MAGIC 12. Now, select the **Grant** option.
# MAGIC 13. In the **Principals** field, **search for the identity** using the email or name.
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * The identity appears in the drop-down, confirming it as a valid principal in Unity Catalog.
# MAGIC   * Privileges on data objects can now be granted to this user.
# MAGIC   * However, this user cannot perform actions until it is assigned to a workspace.
# MAGIC   * This step shows the user can have privileges granted, even though they lack workspace access.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Deleting the Custom Catalog
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **Catalog**.
# MAGIC 3. It will open the **Catalog Explorer** page where you will view the list of available catalogs in the **left catalog pane**.
# MAGIC 4. Select the custom catalog **test_catalog** to access its details in the right section of the screen.
# MAGIC 5. Navigate to the **Schemas** tab in the custom catalog.
# MAGIC 6. Select the **default** schema.
# MAGIC 7. Click the **kebab menu** at the top right, and select the **Delete** option.
# MAGIC 8. You'll be prompted to confirm. Select the **Delete** option to confirm the action.
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * The **information_schema** does not need to be deleted as it is managed by Unity Catalog.
# MAGIC   * Once the default schema is deleted, you will be returned to the **Schemas** tab for the custom catalog.
# MAGIC
# MAGIC 9. Select the **kebab menu** at the top right corner of this page and select the **Delete** option for the catalog.
# MAGIC 10. Again, you will be prompted to confirm. Select the **Delete** option to confirm the action.
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * This will delete your catalog and you will be returned to the **Catalog Explorer** page with the list of the available catalogs.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Deleting a User
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **User Management**.
# MAGIC 3. Ensure that you are under the **User** tab.
# MAGIC 4. Locate the target user using the search field if required. \(**For Example:** In this case, it is **Test User**\)
# MAGIC 5. Select the target user **Test User** to access its details page.
# MAGIC 6. Select the **kebab menu** at the top right corner of the page and select the **Delete User** option.
# MAGIC 7. You'll be prompted to confirm. Review the prompt and then, select **Cancel** to keep the user intact for the upcoming demos. **Confirming delete** will remove that user.
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * If a user leaves, they can be deleted, and ownership of data objects remains unaffected.
# MAGIC   * If they are merely absent for an extended period of time, but we wish to temporarily revoke access, then users can also be deactivated, although this option is only available through the API at the present time.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Conclusion
# MAGIC
# MAGIC You have successfully learned how to add and remove users in the Databricks account console to keep workspace access aligned with organizational needs and lifecycle events. This demo showed how to onboard new identities, update basic user details, and cleanly remove users who no longer require access, ensuring your environment remains secure and well-governed.
# MAGIC
# MAGIC Key takeaways from this demo include:
# MAGIC
# MAGIC - **User accounts** represent individual human identities that authenticate to Databricks and consume workspace resources.  
# MAGIC - **Adding users** through the account console or identity integrations ensures consistent onboarding and traceable ownership for workspaces and data assets.  
# MAGIC - **Deleting or deactivating users** helps maintain least-privilege access by revoking accounts that are no longer required.  
# MAGIC - **User lifecycle management** (add, update, remove) is a core responsibility of account admins for auditability and compliance.  
# MAGIC - **Accurate user metadata** (names, emails, roles) improves discoverability, accountability, and collaboration across the Lakehouse.  
# MAGIC
# MAGIC With hands-on experience in adding and deleting users, you can now maintain a clean, well-controlled user directory that underpins secure access to workspaces, compute, and Unity Catalog-governed data.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>