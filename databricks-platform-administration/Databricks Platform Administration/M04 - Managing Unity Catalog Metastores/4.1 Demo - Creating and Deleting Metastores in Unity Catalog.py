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
# MAGIC # Demo - Creating and Deleting Metastores in Unity Catalog
# MAGIC
# MAGIC ## Overview
# MAGIC In this demo, we will learn how to create and delete metastores within Unity Catalog. Through practical steps, you will learn to setup a new metastore, configuring it with the necessary storage paths and credentials, and assigning it to workspaces. We'll also demonstrate the process for deleting a metastore and ensuring data clearance. By the end of this demo, you will be prepared to manage metastores effectively, ensuring proper data organization and security within your Databricks environment.
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
# MAGIC         <li>Completion of the <strong>Managing Principals in the Unity Catalog</strong> demo.</li>
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
# MAGIC * **Stage 1:** Creating a Metastore
# MAGIC * **Stage 2:** Deleting a Metastore
# MAGIC
# MAGIC
# MAGIC #### Note: 
# MAGIC   * These instructions should only be performed in an environment where you have been granted admin access.
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Key Concepts: Metastore
# MAGIC Account administrators create metastores and assign them to workspaces to allow workloads in those workspaces to access the data represented in the metastore.
# MAGIC
# MAGIC This can be done in the Account Console, through REST APIs, or using Terraform.
# MAGIC
# MAGIC In this demo, we'll explore the creation and management of metastores interactively using the Account Console.
# MAGIC
# MAGIC There are some underlying cloud resources that must be set up by your cloud administrator first in order to support the metastore.
# MAGIC
# MAGIC This includes a cloud storage container to house the data associated with your data objects and a cloud credential that allows Databricks to access that container.
# MAGIC
# MAGIC The creation of these resources is outside the scope of this demo.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Creating a Metastore
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **Catalog**. This will display a list of currently existing **Metastores**.
# MAGIC 3. Locate the **development** metastore using the search field if required.
# MAGIC 4. Select the **development** metastore to access its details page.
# MAGIC 5. Copy the following values from the **development** metastore and **save** them for future reference:
# MAGIC     * **S3 bucket path** \(ignoring the alphanumeric sub-bucket name\) 
# MAGIC     * **IAM role ARN**
# MAGIC 6. Now, In the **left sidebar**, again select **Catalog** to return to the previous page.
# MAGIC 7. In the **Catalog** page, select the **Create metastore** option.
# MAGIC 8. Fill in the metastore details with relevant details as specified below:
# MAGIC | Field Name | Instructions |
# MAGIC |--|--|
# MAGIC | Name | Enter a unique name such as **main-us-east** |
# MAGIC | Region | Choose a region to host the metastore such as **Ohio \(us-east-2\)** |
# MAGIC | S3 bucket path \(optional\) | Enter the **S3 bucket path** that we copied from the **development** metastore in **Step 5** |
# MAGIC | IAM role ARN \(optional\) | Enter the **IAM role ARN** that we copied from the **development** metastore in **Step 5** |
# MAGIC
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * **Name:**
# MAGIC     * Enter a **unique name** for the metastore.
# MAGIC     * You can use any naming convention.
# MAGIC     * The metastore name is not visible to users.
# MAGIC   * **Region:** 
# MAGIC     * Choose the region from the drop-down list in which to host the metastore.
# MAGIC     * The region must align geographically with the workspaces to which the metastore will be assigned.
# MAGIC     * Only one metastore per region is allowed. If the region is unavailable, it may already have a metastore created.
# MAGIC   * **S3 bucket path \(optional\):**
# MAGIC     * This field is optional but required for this demo to set up a metastore with the same **S3 bucket path** as the **development** metastore.
# MAGIC     * Specify the path that your cloud administrator provided for the cloud storage container.
# MAGIC   * **IAM role ARN \(optional\):**
# MAGIC     * This is also an optional field but required for this demo to set up a metastore with the same **IAM role ARN** as the **development** metastore.
# MAGIC     * Enter the IAM role ARN for AWS or access connector ID for Azure.
# MAGIC
# MAGIC   9. Once all information is entered, verify them and select the **Create** option.
# MAGIC   10. You'll be prompted to configure the IAM role. Review the prompt and then, select the **IAM role configured** option.
# MAGIC   11. After creating the metastore, the last stage of the metastore setup will ask you to **Select at least one workspace to proceed**.
# MAGIC   12. Select the **Skip** option.
# MAGIC   13. You will be greeted with a **Congratulations** prompt. Select the **Close** option.
# MAGIC
# MAGIC
# MAGIC ### Assigning the Metastore to Workspaces \(Optional\)
# MAGIC 📌 **Note**: 
# MAGIC   * After creating the metastore, you can assign it to workspaces available in the account.
# MAGIC   * This step is optional and can be done at any time in the future.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Deleting a Metastore
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **Catalog**. This will display a list of currently existing **Metastores**.
# MAGIC 3. Locate the target metastore using the search field if required.  \(**For Example:** In this case, it is **main-us-east** metastore.\)
# MAGIC 4. Select the target metastore **main-us-east** to access its details page.
# MAGIC 6. Select the **kebab menu** at the top right corner of the page and select the **Delete** option.
# MAGIC 7. You’ll be prompted to type the metastore name to confirm deletion. Review the prompt and select **Cancel** to retain the metastore for upcoming demos. **Typing the metastore name** and **confirming delete** will delete that metastore.
# MAGIC
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * To delete a metastore, ensure that it is no longer needed.
# MAGIC   * Deleting a metastore will **only delete the metadata**, not the actual data **unless the storage is cleared**.
# MAGIC   * Before deleting a metastore, ensure that its associated **S3 bucket** is cleared to expunge data from managed tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Conclusion
# MAGIC
# MAGIC You have successfully learned how to create and delete Unity Catalog metastores at the account level to serve as governance boundaries for your data. This demo walked through defining a new metastore, configuring its storage and region, and understanding when it is appropriate to remove a metastore that is no longer needed.
# MAGIC
# MAGIC Key takeaways from this demo include:
# MAGIC
# MAGIC - **Metastores** act as top-level governance containers that hold catalogs, schemas, and tables for one or more workspaces.  
# MAGIC - **Creating a metastore** involves specifying a region and storage location, aligning governance with cloud and compliance requirements.  
# MAGIC - **Deleting a metastore** is a destructive operation that removes associated Unity Catalog metadata and must be planned carefully.  
# MAGIC - **One-metastore-per-region guidance** helps simplify governance and avoid fragmentation of data management.  
# MAGIC - **Metastore lifecycle management** is a key responsibility of account admins to keep the data governance topology clean and intentional.  
# MAGIC
# MAGIC With hands-on experience creating and deleting metastores, you can now design and maintain a Unity Catalog topology that matches your organization's regions, compliance domains, and workspace architecture.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>