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
# MAGIC # Demo - Creating a Cluster in Unity Catalog
# MAGIC
# MAGIC ## Overview
# MAGIC In this demo, we will learn how to create and configure a cluster within Unity Catalog in Databricks. Through a series of steps, we'll cover cluster creation, configuration of cluster modes, access settings, and Databricks runtime selection. We'll also explore how to configure instance profiles and other necessary settings to ensure proper connectivity with Unity Catalog. By the end of this demo, you will have the skills to set up and manage clusters in Databricks, enabling secure and efficient access to your data resources.
# MAGIC
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this demo, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Identify the basic steps involved in creating a cluster in Unity Catalog</li>
# MAGIC         <li>Differentiate between cluster modes and access modes in Unity Catalog</li>
# MAGIC         <li>Configure an all-purpose cluster to access Unity Catalog in Databricks</li>
# MAGIC         <li>Discuss the associated configurations in detail for creating a cluster</li>
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
# MAGIC * **Stage 1:** Create a Cluster in Unity Catalog
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
# MAGIC In this Demo, we see how to configure an all-purpose cluster to access Unity Catalog.
# MAGIC
# MAGIC The same configuration principles can also be applied when configuring job clusters for running automated workloads.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A1. Architectural Comparison
# MAGIC Prior to Unity Catalog, a full data governance solution required careful configuration of workspace settings, access control lists, and cluster settings and policies. It was a cooperative system that, if not properly set up, could allow users to bypass access control altogether.  
# MAGIC
# MAGIC While Unity Catalog introduces some new settings, the system does not require any specific configuration to be secure.
# MAGIC
# MAGIC Without proper settings, clusters will not be able to access any secure data at all, making Unity Catalog secure by default. This, coupled with its improved fine-grained control and auditing, makes Unity Catalog a significantly involved data governance solution.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### A2. Cluster Overview
# MAGIC In order to work with data in Databricks, we need to use compute resources which include clusters and warehouses. In this demo, we will focus on clusters.
# MAGIC
# MAGIC Apart from SQL warehouses, clusters are the gateway to your data, as they are the workhorse that's responsible for running the code in your notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Creating Classic Compute Clusters in Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Steps to Create a Cluster in Unity Catalog
# MAGIC 1. Log into the **Account Console** as an **Account Administrator**.
# MAGIC 2. In the **left sidebar**, select **Workspace**.
# MAGIC 3. Locate the desired workspace using the search field, if required. \(For Example, in this case enter **test-workspace**\).
# MAGIC 4. Select the **test-workspace** to access its details page.
# MAGIC 5. Ensure that you are under the **Configuration** tab.
# MAGIC 6. Select the **URL** for the workspace on the right side of the **Configuration** page. This will redirect you to that Databricks Workspace.
# MAGIC 7. In the **left sidebar** within the newly opened Databricks Workspace, select **Compute**.
# MAGIC 8. Ensure that you are under the **All-purpose compute** tab.
# MAGIC 9. Select the **Create compute** option to start the cluster creation process. You will be taken to the **New compute** page.
# MAGIC 10. At the top of the **New compute** page, you will find the name assigned for the new cluster. Click on the **edit icon** next to this name.
# MAGIC 11. Provide a unique name for your new cluster. In this case name it as **test_cluster**.
# MAGIC 12. Fill in the remaining cluster details in this page with relevant details as specified below:
# MAGIC | Field Name | Instructions |
# MAGIC |--|--|
# MAGIC | Node | Set the compute type to **Single node**. |
# MAGIC | Access mode | Select **Single user** option from the drop-down list. |
# MAGIC | Single user access | It is a search field that also functions as a drop-down to find users. <br/>Enter **Test User** in this field. |
# MAGIC | Databricks runtime version | It is a drop-down list with two categories: **Standard** and **ML**. <br/>Go to the **Standard** category and select the **15.4 LTS \(Scala 2.12, Spark 3.5.0\)** runtime. |
# MAGIC | Use Photon Acceleration | **Enable** this feature by **selecting its checkbox**. |
# MAGIC | Node type | It is a drop-down list with **various instance types** grouped in **different categories**. <br/>Select the **i3.xlarge \(30.5 GB Memory, 4 Cores\)** instance type under the **Storage optimized (Delta cache accelerated\)** category. |
# MAGIC | Enable autoscaling local storage | **Enable** this feature by **selecting its checkbox**. |
# MAGIC | Terminate after ___ minutes of inactivity | **Enable** this feature by **selecting its checkbox** and set the duration to **120** minutes. |
# MAGIC | Instance profile | It is a drop-down list. Set this to **None**. |
# MAGIC | Tags | Do not modify anything in this field. |
# MAGIC | Advanced options | Do not modify anything in any field under this section. |
# MAGIC
# MAGIC 13. Once all information is entered, verify them and select the **Create compute** option.
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * This operation may take a few moments. Meanwhile, you can view the cluster configuration summary in the top-right Summary box.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Brief Summary of the Fields Available in the **New Compute** Page:
# MAGIC | Field Name | Description |
# MAGIC |--|--|
# MAGIC | Node | It has two types of compute categories: **Multi node** and **Single node**. |
# MAGIC | Access mode | It has three types of access modes: **Single user**, **Shared**, and **No isolation shared**. |
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * **Single user** access runs SQL, Python, R, and Scala workloads as a single user, with access to data secured in the Unity Catalog.
# MAGIC   * In **Shared** access, multiple users can share the cluster to run SQL, Python, and Scala workloads on data secured in the Unity Catalog.
# MAGIC   * And, in **No isolation shared** access, multiple users can share the cluster to run workloads in any language, with no isolation between users.
# MAGIC   * If we select the **Single user** option for the access mode, it gives us another field for **Single user access** to select the target user account to provide the access; however, if we select any of the other access modes, this field disappears.
# MAGIC   * For this demo, we need a single user cluster, so set the **Access mode** field to **Single user** from its drop-down list and also set the **Single user access** field next to it to your user account.
# MAGIC
# MAGIC
# MAGIC | Field Name | Description |
# MAGIC |--|--|
# MAGIC | Single user access | It is a search field that also functions as a drop-down to find users. |
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * It is used to provide the cluster access to a specific user.
# MAGIC   * It provides at least the **Can Attach To** permission to the **assigned user** after cluster creation.
# MAGIC   * It also provides the **Can Manage** permission to the **Admins** and the **Cluster Creator** but they cannot run commands on this cluster.
# MAGIC
# MAGIC | Field Name | Description |
# MAGIC |--|--|
# MAGIC | Databricks runtime version | It is a drop-down list with two categories: **Standard** and **ML**. <br/>Both of these have their own set of **databricks runtime images** that will be used to create a cluster. |
# MAGIC | Use Photon Acceleration | It is a **check box** to enable the **Photon Acceleration** feature along with the selected runtime version. |
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * The Databricks runtime is a collection of core software components running on the cluster, including Apache Spark and many other components and updates that substantially improve the usability, performance, and security of big data analytics.
# MAGIC   * There is also an add-on called Photon for optimizing the performance and cost of SQL workloads. Databricks also offers an augmented runtime catering to the needs of machine learning workloads.
# MAGIC   * Select the most recent stable version available. At the time of this demo, 15.4 LTS is selected.
# MAGIC   * For Unity Catalog connectivity, the minimum required version is 10.1, with some features requiring newer versions.
# MAGIC
# MAGIC
# MAGIC | Field Name | Description |
# MAGIC |--|--|
# MAGIC | Node type | It is a drop-down list with **various instance types** grouped in **different categories**. <br/>This setting enables us to select the required instance type to use disk caching. |
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * Databricks recommends instance types with local SSDs to take advantage of disk caching.
# MAGIC   * The disk cache is configured to use at most half of the space available on the local SSDs provided with the worker nodes.
# MAGIC
# MAGIC | Field Name | Description |
# MAGIC |--|--|
# MAGIC | Enable autoscaling local storage | It is a **check box** to enable the **autoscaling local storage** feature in your AWS account. |
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * Enabling this feature will create additional EBS volumes in your AWS account.
# MAGIC   * These volumes will not be deleted until the instances they are attached to are deleted.
# MAGIC   * For this reason, Databricks recommends enabling this feature on autoscaling or autoterminating clusters.
# MAGIC
# MAGIC | Field Name | Description |
# MAGIC |--|--|
# MAGIC | Terminate after ___ minutes of inactivity | It is a **check box** to enable the **cluster time-out** feature and to set up the **specified time interval of inactivity** post which the cluster will be terminated automatically. |
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * When enabled, the compute will terminate after the specified time interval of inactivity \(i.e., no running commands or active job runs\).
# MAGIC   * This feature is best supported in the latest Spark versions.
# MAGIC   * The default duration is set to **120 minutes**.
# MAGIC
# MAGIC | Field Name | Description |
# MAGIC |--|--|
# MAGIC | Instance profile | This is a feature with a **drop-down field** that allows you to access your data from Databricks clusters without the need to manage, deploy, or rotate AWS keys. |
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * New instance profiles can be added in the Admin Console.
# MAGIC
# MAGIC | Field Name | Description |
# MAGIC |--|--|
# MAGIC | Tags | This feature is a **collection of key-value pairs** that are automatically added to cluster instances for tracking usage in your cloud provider. |
# MAGIC
# MAGIC 📌 **Note**: 
# MAGIC   * This compute uses an instance pool.
# MAGIC   * If no tags are specified, then the cloud provider instance tags are set from the pool's tags automatically.
# MAGIC   * When using a pool, cluster tags are shown in your DBU usage details but are not added to provider instance tags.
# MAGIC   * You can expand the **Automatically added tags** option to see all the tags that will be added automatically.
# MAGIC
# MAGIC | Field Name | Description |
# MAGIC |--|--|
# MAGIC | Advanced options | This section contains some advanced settings that we won't modify in this demo, so they will remain unchanged. |

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Conclusion
# MAGIC
# MAGIC You have successfully learned how to create Unity Catalog-aware clusters that can access governed data while honoring catalogs, schemas, and policies. This demo guided you through selecting a cluster policy, configuring runtime and access modes, and ensuring that the cluster is compatible with Unity Catalog requirements.
# MAGIC
# MAGIC Key takeaways from this demo include:
# MAGIC
# MAGIC - **Unity Catalog-enabled clusters** must use supported access modes and runtimes to enforce catalog- and schema-level security.  
# MAGIC - **Cluster policies** standardize configurations such as instance types, runtimes, and security settings across teams.  
# MAGIC - **Choosing an appropriate access mode** (for example, shared or single user) affects how data permissions are evaluated.  
# MAGIC - **Attaching clusters to Unity Catalog** allows notebooks and jobs to query governed tables, views, and volumes securely.  
# MAGIC - **Lifecycle management of clusters** (create, edit, terminate) directly impacts cost, performance, and compliance.  
# MAGIC
# MAGIC With hands-on experience creating Unity Catalog–compatible clusters, you can now provision compute that respects centralized governance while supporting scalable analytics and data engineering workloads.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>