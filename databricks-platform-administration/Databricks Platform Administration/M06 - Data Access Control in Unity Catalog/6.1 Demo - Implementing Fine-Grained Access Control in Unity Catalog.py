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
# MAGIC # Demo - Implementing Fine-Grained Access Control in Unity Catalog
# MAGIC
# MAGIC ## Overview
# MAGIC In this demo, you will implement fine-grained access control for core HR data objects in Unity Catalog, from environment setup through policy-driven governance using tags, UDFs, and ABAC. You will create a dedicated ABAC demo catalog and a secure schema, define and populate sensitive employee tables, and enrich them with both business metadata and governed tags to describe sensitivity, ownership, and region. You will then author reusable user-defined functions for column masking and row filtering, use them with legacy column masks with row filters, and dynamic views, and finally implement modern attribute-based access control (ABAC) policies that automatically apply masking and filtering based on governed tags. By the end, you will have hands-on experience inspecting and managing privileges, understanding the impact of ownership vs. grants, and comparing legacy view/table controls with centralized, tag-driven ABAC policies for scalable, fine-grained data protection in Unity Catalog.
# MAGIC
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this demo, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Describe how Unity Catalog structures governance using catalogs, schemas, tables, and views</li>
# MAGIC         <li>Explain how governed tags and metadata classify and protect sensitive HR data in Unity Catalog</li>
# MAGIC         <li>Outline how user-defined functions support column masking and row-level filtering for different user groups</li>
# MAGIC         <li>Explain how attribute-based access control policies centralize and scale fine-grained data protection across tagged objects</li>
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
# MAGIC         <li>Cloud resources to support the <strong>metastore</strong>.</li>
# MAGIC         <li>Have <strong>metastore admin</strong> capability in order to create and manage a catalog</li>
# MAGIC         <li>Access to a <strong>Unity-Catalog enabled Databricks Workspace</strong> with the ability to create catalogs in your metastore.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #4caf50; background: #e8f5e9; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">⚙️</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #2e7d32; font-size: 1.1em;">Requirements</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">To run this notebook, you need to meet the following technical considerations:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Databricks runtime(s): <strong>17.3.x-scala2.13</strong>. You can also use a <strong>serverless compute (v4 or above)</strong> to run this notebook.</li>
# MAGIC         <li><strong>Unity Catalog</strong> enabled workspace</li>
# MAGIC         <li><strong>Serverless</strong> compute enabled</li>
# MAGIC         <li><strong>CREATE CATALOG</strong> and <strong>USE CATALOG</strong> privileges in the metastore in order to create and manage catalogs</li>
# MAGIC         <li><strong>CREATE SCHEMA</strong> and <strong>USE SCHEMA</strong> privileges in the metastore in order to create and manage schemas</li>
# MAGIC         <li><strong>CREATE TABLE</strong> privileges on catalog schema(s) to create indexes</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this notebook.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. Compute Requirements
# MAGIC
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🚨</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #c62828; font-size: 1.1em;">REQUIRED – SELECT CLASSIC OR SERVERLESS COMPUTE</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">This notebook runs on both <strong>Classic</strong> and <strong>Serverless</strong> compute. Select your preferred compute resource before executing any cells.</p>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;"><strong>To select your compute resource:</strong></p>
# MAGIC       <ol style="margin: 8px 0 0 20px; padding-left: 20px; color: #333;">
# MAGIC         <li style="margin: 0; padding: 0;">Click the <strong>Connect</strong> drop-down menu at the top-right of the notebook (default: <strong>Serverless</strong>).</li>
# MAGIC         <li style="margin: 0; padding: 0;">Select your compute cluster from the list. If your cluster is not visible, click <strong>More</strong>, then use the <strong>Attach to an existing compute resource</strong> pop-up to locate and select it.</li>
# MAGIC         <li style="margin: 0; padding: 0;">If your classic cluster has terminated, restart it first: Right-click <strong>Compute</strong> in the left navigation pane and select <em>Open in new tab</em>. Find your cluster and click the triangle icon to start it. Wait a few minutes for the cluster to run, then return and complete the selection above.</li>
# MAGIC       </ol>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;"><strong>Testing configuration:</strong> This demo was tested using Databricks runtime <strong>17.3.x-scala2.13</strong> and Serverless compute <strong>version 4</strong>. For more details on Serverless versions, see the <a href="https://docs.databricks.com/aws/en/compute/serverless/dependencies" style="color: #c62828; text-decoration: none; font-weight: 500;">Databricks documentation</a>.</p>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### A2. Install Dependencies
# MAGIC
# MAGIC As part of the workspace setup, a `CONFIG` object is created to dynamically reference environment-specific information needed to run the course, such as **username** and **catalog names**. The configuration is constructed as a **Python dictionary** for direct use in PySpark code and is automatically converted into a **SQL Map Variable** for seamless access within SQL code blocks.
# MAGIC
# MAGIC **Setup includes:**
# MAGIC
# MAGIC 1. **Creates a `CONFIG` object** as both a Python dictionary and SQL Map Variable to store major variables required for this notebook.
# MAGIC 2. **Generates a unique base catalog name** for each user based on their username (e.g., `db_<username>`).
# MAGIC 3. **Generates domain-specific catalog names** for the demo contexts (e.g., `db_abac_<username>`).
# MAGIC 4. **Sanitizes all names** to ensure they are safe for use in Unity Catalog, handling special characters and length limits.
# MAGIC 5. **Captures environment metadata**, including the current user's name and the timestamp of creation.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-1

# COMMAND ----------

# MAGIC %md
# MAGIC ### A3. Inspect Configuration
# MAGIC Run the code blocks below to verify the specific catalog names and user details generated for your current environment:

# COMMAND ----------

# Inspect the CONFIG dictionary values available to PySpark code
# (Works with both Classic Compute and Serverless Compute)
print(f"Username:               {CONFIG['username']}")
print(f"Default Catalog Name:   {CONFIG['catalog_name']}")
print(f"ABAC Catalog Name:      {CONFIG['abac_catalog_name']}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect the CONFIG map variable values available to SQL code
# MAGIC -- (Works with both Classic Compute and Serverless Compute)
# MAGIC SELECT stack(3,
# MAGIC   'User Name',              CONFIG['username'],
# MAGIC   'Default Catalog Name',   CONFIG['catalog_name'],
# MAGIC   'ABAC Catalog Name',      CONFIG['abac_catalog_name']
# MAGIC ) AS (`Property Type`, Value);

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Understanding Account Group Membership and Security Context
# MAGIC
# MAGIC Unity Catalog uses `is_account_group_member()` function to check account-level group membership for conditional security.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Verify Group Memberships
# MAGIC Run the following cell to check the current group memberships for `Users`, `users`, and `admin` groups based on your workspace configuration:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check current group memberships
# MAGIC -- These will vary based on your workspace configuration
# MAGIC SELECT
# MAGIC   is_account_group_member('users') as is_users_member,
# MAGIC   is_account_group_member('admins') as is_admins_member,
# MAGIC   -- Group names are case sensitive
# MAGIC   is_account_group_member('Users') as is_Users_member_case_sensitive;

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Create ABAC Demo Catalog, Schema, and Seed Employee Tables
# MAGIC Let's build a realistic governance structure by creating an ABAC demo catalog and a secure HR schema for sensitive data. These objects will hold representative employee records with PII fields, setting the foundation for hands-on, tag-driven security and access policy demonstrations. This mirrors real HR governance needs and ensures practical, business-aligned ABAC testing.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Creating and Tagging the Catalog
# MAGIC Let's start by creating catalog and tag it for governance.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create an `CONFIG['abac_catalog_name']` catalog for ABAC and attribute-based governance demo datasets, if not already present
# MAGIC CREATE CATALOG IF NOT EXISTS IDENTIFIER(CONFIG['abac_catalog_name'])
# MAGIC COMMENT 'Catalog for ABAC and attribute-based governance demo datasets';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the current catalog for subsequent operations
# MAGIC USE CATALOG IDENTIFIER(CONFIG['abac_catalog_name']);

# COMMAND ----------

# Desired tags for the catalog
desired_tags = {
    "department": "data_governance",
    "env": "demo",
    "owner": "data_eng",
    "purpose": "abac_demo"
}

# 1) Fetch existing tags on the catalog
rows = spark.sql(f"""
  SELECT tag_name, tag_value
  FROM information_schema.catalog_tags
  WHERE catalog_name = '{CONFIG["abac_catalog_name"]}'
""").collect()
existing = {r["tag_name"]: r["tag_value"] for r in rows}

# 2) Unset only keys that already exist (avoids UNSET errors)
for key in desired_tags.keys():
    if key in existing:
        spark.sql(f"UNSET TAG ON CATALOG {CONFIG['abac_catalog_name']} `{key}`")

# 3) Set desired values (use backquotes to avoid parser confusion)
for key, value in desired_tags.items():
    spark.sql(f"SET TAG ON CATALOG {CONFIG['abac_catalog_name']} `{key}` = `{value}`")


# Grant ALL PRIVILEGES and MANAGE on the catalog to the current user
spark.sql(f"""
  GRANT ALL PRIVILEGES, MANAGE
  ON CATALOG {CONFIG['abac_catalog_name']}
  TO `{CONFIG['username']}`
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Creating and Tagging the Schema
# MAGIC
# MAGIC Now, let's create a `securedata` schema for sensitive HR data and tag it for governance.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a 'securedata' schema for sensitive employee-related data, if not already present
# MAGIC CREATE SCHEMA IF NOT EXISTS securedata COMMENT 'Schema for sensitive employee and HR data';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set current schema for all subsequent table operations
# MAGIC USE SCHEMA securedata;

# COMMAND ----------

# Desired tags for the schema
desired_tags = {
    "department": "hr",
    "sensitivity": "high",
    "owner": "hr_team",
    "purpose": "sensitive_employee_data"
}

# 1) Fetch existing tags on the schema
rows = spark.sql("""
  SELECT tag_name, tag_value
  FROM information_schema.schema_tags
  WHERE schema_name = 'securedata'
""").collect()
existing = {r["tag_name"]: r["tag_value"] for r in rows}

# 2) Unset only keys that already exist (avoids UNSET errors)
for key in desired_tags.keys():
    if key in existing:
        spark.sql(f"UNSET TAG ON SCHEMA securedata `{key}`")

# 3) Set desired values (use backquotes to avoid parser confusion)
for key, value in desired_tags.items():
    spark.sql(f"SET TAG ON SCHEMA securedata `{key}` = `{value}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ### C3. Verifying the Current Catalog, Schema, and User

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View and confirm the current catalog, schema, and user context
# MAGIC SELECT
# MAGIC   current_catalog() AS `Current Catalog`,
# MAGIC   current_schema() AS `Current Schema`,
# MAGIC   CURRENT_USER() AS `Current User`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### C4. Create Sample Tables
# MAGIC Create core and expanded employee tables that will be used throughout the demo.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create core employee table
# MAGIC CREATE TABLE IF NOT EXISTS employees (
# MAGIC   employee_id INT,
# MAGIC   name STRING,
# MAGIC   email STRING,
# MAGIC   region STRING,
# MAGIC   salary DOUBLE,
# MAGIC   ssn STRING
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create expanded HR employee table
# MAGIC CREATE TABLE IF NOT EXISTS hr_employees (
# MAGIC   employee_id INT,
# MAGIC   name STRING,
# MAGIC   ssn STRING,
# MAGIC   email STRING,
# MAGIC   salary DECIMAL(10,2),
# MAGIC   region STRING,
# MAGIC   department STRING,
# MAGIC   hire_date DATE,
# MAGIC   performance_rating DECIMAL(3,2)
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Expanded employee master data with PII, department, and compensation info';

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Using Governed Tags
# MAGIC Databricks governed tags are account-level attributes \(such as sensitivity, region, or domain\) that are assigned to Unity Catalog objects \(catalogs, schemas, tables, columns\) to drive attribute-based access control policies. They enable consistent, scalable governance by serving as the trigger for dynamic row filters and column masks in ABAC.
# MAGIC
# MAGIC **Key Permissions:**
# MAGIC
# MAGIC - **Who can create governed tags?**
# MAGIC   - Account admins have `CREATE` and `MANAGE` permissions on governed tags at the account level by default.
# MAGIC   - Workspace admins have `CREATE` permission at the account level by default.
# MAGIC   - Any user or group explicitly granted `CREATE` permission at the account level in Governed Tag permissions.
# MAGIC
# MAGIC - **Who can assign governed tags?**
# MAGIC   - Any user or group with the `ASSIGN` permission on governed tags at the account level and the `APPLY TAG` privilege on the target Unity Catalog object.
# MAGIC
# MAGIC **Lab Environment Setup:**
# MAGIC
# MAGIC - In this lab environment, you do **not** have **Account Admin** or **Workspace Admin** access.
# MAGIC
# MAGIC - Your environment includes two pre-created governed tags:
# MAGIC   - `pii` with allowed values: `ssn`, `email`, `address`
# MAGIC   - `classification` with allowed value: `salary_masked`
# MAGIC
# MAGIC - You have been granted `ASSIGN` permission to apply these tags throughout this demo. The steps below demonstrate the tag creation process for future reference when you have **Account Admin** or **Workspace Admin** privileges in your own environment.
# MAGIC
# MAGIC > **Note:** Governed tags can only be created through the Databricks UI; programmatic creation via SQL or Python APIs is not currently supported.

# COMMAND ----------

# MAGIC %md
# MAGIC **Steps to Create Governed Tags in UI (for Account Admins and Workspace Admins):**
# MAGIC - In Databricks workspace, open **Catalog Explorer**.
# MAGIC - Click **Governance** option in the top
# MAGIC - You will be taken to **Governance** page and its **Governed Tags** tab
# MAGIC - Click **Create governed tag** at the top right corner of this page.
# MAGIC - A dialog box for **Create governed tag** will open. Provide the following values:
# MAGIC   - Key: pii
# MAGIC   - Description: "Personally identifiable information"
# MAGIC   - Allowed values: ssn, email, address *(each will be added seperately)*
# MAGIC - Again define another key as:
# MAGIC   - Key: classification
# MAGIC   - Description: "Salary field to be masked"
# MAGIC   - Allowed values: salary_masked
# MAGIC - Click on the **Create** button.

# COMMAND ----------

# MAGIC %md
# MAGIC Now you can proceed with the subsequent steps in this lab. They only rely on the pre-created governed tags and your `ASSIGN` permission, and do not require any additional admin or tag-creation privileges.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Assign Governed Tags (SQL)
# MAGIC Apply the pre-created governed tags to sensitive columns in the `employees` table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tag the SSN column as PII
# MAGIC ALTER TABLE employees ALTER COLUMN ssn SET TAGS ('pii' = 'ssn');
# MAGIC
# MAGIC -- Tag the EMAIL column as PII
# MAGIC ALTER TABLE employees ALTER COLUMN email SET TAGS ('pii' = 'email');
# MAGIC
# MAGIC -- Tag the SALARY column with a governed classification tag
# MAGIC ALTER TABLE employees ALTER COLUMN salary SET TAGS ('classification' = 'salary_masked');
# MAGIC
# MAGIC -- Tag the TABLE as region-specific
# MAGIC ALTER TABLE employees SET TAGS ('region' = 'EMEA');

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Programmatically Assign Table Tags
# MAGIC Add business metadata tags to `hr_employees` and governed PII tags to specific columns.

# COMMAND ----------

# --- Assign business/governance tags (non-governed) to the table itself ---

# Define table-level tags: general metadata for governance, compliance, or discovery
table_tags = {
    "department": "hr",               # Business department responsible for this table
    "sensitivity": "high",            # Sensitivity classification ("low", "medium", "high", etc.)
    "owner": "hr_team",               # Data owner or steward
    "purpose": "employee_master_data",# Intended use
    "region": "EMEA"                  # Region (entire table classified for region, e.g., for compliance jurisdiction)
}

# Fetch all existing tags for this table to avoid errors if unsetting a tag that's not present
rows = spark.sql("""
  SELECT tag_name, tag_value
  FROM information_schema.table_tags
  WHERE table_name = 'hr_employees'
""").collect()
existing = {r["tag_name"]: r["tag_value"] for r in rows}

# Remove only those table-level tags that already exist (avoids "tag not found" errors)
for key in table_tags.keys():
    if key in existing:
        spark.sql(f"UNSET TAG ON TABLE hr_employees `{key}`")

# Set all table-level tags
for key, value in table_tags.items():
    spark.sql(f"SET TAG ON TABLE hr_employees `{key}` = `{value}`")

# --- Assign governed sensitivity tags (like pii) to specific columns ---

# For each sensitive column, associate the appropriate governed tag and allowed value
column_tags = {
    "ssn": {"pii": "ssn"},      # Column "ssn" contains SSN and must use allowed value for "pii" tag
    "email": {"pii": "email"},  # Column "email" contains email addresses and must use allowed value for "pii"
    # Do NOT assign "region" as a column tag unless using for masking/classification; it is for data, not column-level ABAC
}

for column, tags in column_tags.items():
    for key, value in tags.items():
        # Idempotently UNSET, ignoring missing tags (try-except for robustness)
        try:
            spark.sql(f"UNSET TAG ON COLUMN hr_employees.{column} `{key}`")
        except Exception:
            pass  # It's OK if the tag wasn't set before
        # SET governed tag with allowed value
        spark.sql(f"SET TAG ON COLUMN hr_employees.{column} `{key}` = `{value}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Populate Tables with Dummy Data
# MAGIC Add realistic test employee records to both tables so you can verify data access controls and ABAC policy behavior in later steps.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E1. Insert Sample Employee Records
# MAGIC Run the following cell to insert sample employee records with basic sensitive data:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert dummy employee records into employees table of IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.employees')
# MAGIC INSERT INTO IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.employees') VALUES
# MAGIC (1, 'Nancy Gibson', 'ngibs02@example.com', 'EMEA', 84500.00, '666-84-2234'),
# MAGIC (2, 'Andrew Roberts', 'arobe01@example.com', 'Americas', 110450.00, '900-33-2748'),
# MAGIC (3, 'Nobu Yagawa', 'nobu@example.com', 'Asia Pacific', 98000.00, '921-40-2534'),
# MAGIC (4, 'Ines Drechsler', 'idrec01@example.com', 'EMEA', 156500.00, '966-25-0367'),
# MAGIC (5, 'Arif Handal', 'ahand03@example.com', 'EMEA', 82500.00, '962-62-8977'),
# MAGIC (6, 'Sarah Johnson', 'sjohnson@example.com', 'Americas', 125000.00, '123-45-6789'),
# MAGIC (7, 'David Kim', 'dkim@example.com', 'Asia Pacific', 93000.00, '987-65-4321');

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2. Verify Basic Employee Records
# MAGIC Run the following cell to verify the records:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.employees') ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### E3. Insert Expanded Employee Details
# MAGIC Run the following cell to insert expanded employee details with department and performance ratings:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert detailed employee records into the hr_employees table of IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.hr_employees')
# MAGIC
# MAGIC INSERT INTO IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.hr_employees') VALUES
# MAGIC (1, 'Nancy Gibson', '666-84-2234', 'ngibs02@example.com', 84500.00, 'EMEA', 'Engineering', '2020-01-15', 4.2),
# MAGIC (2, 'Andrew Roberts', '900-33-2748', 'arobe01@example.com', 110450.00, 'Americas', 'Sales', '2019-03-22', 4.7),
# MAGIC (3, 'Nobu Yagawa', '921-40-2534', 'nobu@example.com', 98000.00, 'Asia Pacific', 'Engineering', '2021-06-10', 4.1),
# MAGIC (4, 'Ines Drechsler', '966-25-0367', 'idrec01@example.com', 156500.00, 'EMEA', 'Management', '2018-11-08', 4.9),
# MAGIC (5, 'Arif Handal', '962-62-8977', 'ahand03@example.com', 82500.00, 'EMEA', 'Marketing', '2022-02-14', 3.8),
# MAGIC (6, 'Sarah Johnson', '123-45-6789', 'sjohnson@example.com', 125000.00, 'Americas', 'HR', '2020-07-20', 4.5),
# MAGIC (7, 'David Kim', '987-65-4321', 'dkim@example.com', 93000.00, 'Asia Pacific', 'Finance', '2021-09-12', 4.0);

# COMMAND ----------

# MAGIC %md
# MAGIC ### E4. Verify Expanded Employee Data
# MAGIC Run the following cell to verify the expanded data:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.hr_employees') ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Creating and Managing User-Defined Functions
# MAGIC Create user-defined functions (UDFs) for column masking and row filtering that can be reused by legacy controls and ABAC policies.

# COMMAND ----------

# MAGIC %md
# MAGIC ### F1. Create a Column Masking UDF
# MAGIC Run the following cell to create a UDF for Column Masking:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mask SSN for everyone except compliant users
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.mask_ssn')(ssn STRING) RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('Users') THEN ssn
# MAGIC   ELSE 'XXX-XX-XXXX'
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC ### F2. Create a Row Filter UDF
# MAGIC Run the following cell to create a UDF for Row Filtering:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row filter: restrict high-salary rows for non-privileged users
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.filter_by_salary')(salary DOUBLE) RETURNS BOOLEAN
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('Users') THEN TRUE                              -- 'Users' see all rows
# MAGIC   WHEN salary <= 100000 THEN TRUE                                -- others see only lower-salary rows
# MAGIC   ELSE FALSE
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC ### F3. Analyze the Function Properties with `DESCRIBE`
# MAGIC Let's use the **`DESCRIBE`** statement to get information about the functions we just created.
# MAGIC
# MAGIC Run the following cells to perform this task:

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.mask_ssn')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.filter_by_salary')

# COMMAND ----------

# MAGIC %md
# MAGIC ### F4. Run the User-Defined Functions
# MAGIC
# MAGIC Validate the masking and row filter functions in isolation before attaching them to tables or policies.
# MAGIC
# MAGIC Run the following cells to perform this operation:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the mask_ssn UDF with different sample values
# MAGIC SELECT
# MAGIC   '123-45-6789' AS original_ssn,
# MAGIC   IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.mask_ssn')('123-45-6789') AS masked_ssn;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the filter_by_salary UDF with a range of salaries
# MAGIC SELECT
# MAGIC   s AS test_salary,
# MAGIC   IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.filter_by_salary')(s) AS is_visible
# MAGIC FROM VALUES
# MAGIC   (80000.0),
# MAGIC   (100000.0),
# MAGIC   (120000.0) AS t(s);

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Access Control and Privilege Management
# MAGIC
# MAGIC Unity Catalog uses an explicit permission model, where access must be granted separately at the **catalog**, **schema**, and **object** (table or view) levels. This section inspects, grants, and revokes access on the objects created in this demo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### G1. Inspecting Current Privileges for the Current User
# MAGIC
# MAGIC Before making any changes, inspect the privileges your current user already has on the ABAC catalog, the `securedata` schema, and the `employees` table. This establishes a baseline for catalog, schema, and table-level access.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Privileges on the ABAC catalog for the current user
# MAGIC SELECT *
# MAGIC FROM system.information_schema.catalog_privileges
# MAGIC WHERE catalog_name = CONFIG['abac_catalog_name']
# MAGIC   AND grantee = CURRENT_USER();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Privileges on the securedata schema for the current user
# MAGIC SELECT *
# MAGIC FROM system.information_schema.schema_privileges
# MAGIC WHERE catalog_name = CONFIG['abac_catalog_name']
# MAGIC   AND schema_name = 'securedata'
# MAGIC   AND grantee = CURRENT_USER();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Privileges on the hr_employees view for the current user
# MAGIC SELECT *
# MAGIC FROM system.information_schema.table_privileges
# MAGIC WHERE table_catalog = CONFIG['abac_catalog_name']
# MAGIC   AND table_schema  = 'securedata'
# MAGIC   AND table_name    = 'employees'
# MAGIC   AND grantee       = CURRENT_USER();

# COMMAND ----------

# MAGIC %md
# MAGIC ### G2. Granting Access to Catalog and Schema for a User
# MAGIC
# MAGIC To allow any user to access data objects, Unity Catalog requires **USE CATALOG** on the catalog and **USE SCHEMA** on the schema that contains those objects. The following commands show how to grant these privileges explicitly to a user whose name is stored in `CONFIG['username']`. You can adapt the pattern to grant access to any other user.

# COMMAND ----------

# Grant USE CATALOG on the ABAC catalog to the user in CONFIG['username']
spark.sql(f"GRANT USE CATALOG ON CATALOG `{CONFIG['abac_catalog_name']}` TO `{CONFIG['username']}`")

# COMMAND ----------

spark.sql(f"GRANT USE SCHEMA ON SCHEMA `{CONFIG['abac_catalog_name']}`.securedata TO `{CONFIG['username']}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ### G3. Revoking and Re‑Granting SELECT on the View
# MAGIC
# MAGIC In this step, you first revoke **SELECT** on the `employees` table and see the effect, then grant **SELECT** back to restore access.

# COMMAND ----------

# MAGIC %md
# MAGIC #### G3(A). Revoking SELECT on the Table
# MAGIC
# MAGIC In this step, you practice revoking **SELECT** on the `employees` table and observe what happens when you query it afterward.

# COMMAND ----------

# Revoke SELECT on the employees table from the user in CONFIG['username']
spark.sql(
    f"REVOKE SELECT ON TABLE `{CONFIG['abac_catalog_name']}`.securedata.employees FROM `{CONFIG['username']}`"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attempt to query the table after SELECT has been revoked
# MAGIC -- This should fail for the user in CONFIG['username'] until SELECT is granted again.
# MAGIC SELECT *
# MAGIC FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.employees')
# MAGIC ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC **Why do we still see rows?**
# MAGIC
# MAGIC In this lab, the current user is also the **owner** of `employees` table, and owners always retain access to their own objects even if an explicit `SELECT` grant is revoked. In a real multi-user workspace, revoking `SELECT` from a different (non-owner) user or group would prevent that user from querying the table until you grant `SELECT` again.

# COMMAND ----------

# MAGIC %md
# MAGIC #### G3(B). Granting SELECT Back on the Table
# MAGIC
# MAGIC Now grant **SELECT** back to the same user to restore their ability to query the table (this is what you would do in a real workspace after re-enabling access).

# COMMAND ----------

# Grant SELECT back on the employees table to the user in CONFIG['username']
spark.sql(
    f"GRANT SELECT ON VIEW `{CONFIG['abac_catalog_name']}`.securedata.employees TO `{CONFIG['username']}`"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the table again after SELECT has been re-granted
# MAGIC -- This should now succeed for the user in CONFIG['username'].
# MAGIC SELECT *
# MAGIC FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.employees')
# MAGIC ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### G4. Function Access and Execution Permissions
# MAGIC
# MAGIC Unity Catalog also controls who can **execute** user-defined functions. Here you revoke, verify, grant, re‑verify, and run the `mask_ssn` UDF.

# COMMAND ----------

# MAGIC %md
# MAGIC #### G4(A). Revoking EXECUTE on mask_ssn
# MAGIC
# MAGIC First, revoke EXECUTE on `mask_ssn` from the user stored in `CONFIG['username']`.

# COMMAND ----------

# Revoke EXECUTE on mask_ssn from the user stored in CONFIG['username']
spark.sql(
    f"REVOKE EXECUTE ON FUNCTION `{CONFIG['abac_catalog_name']}`.securedata.mask_ssn FROM `{CONFIG['username']}`"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### G4(B). Verifying EXECUTE Privileges After Revoke
# MAGIC Check current grants on `mask_ssn` to see which users still have `EXECUTE`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ensure you are in the correct catalog and schema before checking the grants
# MAGIC USE CATALOG IDENTIFIER(CONFIG['abac_catalog_name']);
# MAGIC USE SCHEMA securedata;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show the grants on the 'mask_ssn' function
# MAGIC SHOW GRANTS ON FUNCTION securedata.mask_ssn;

# COMMAND ----------

# MAGIC %md
# MAGIC **Why might the function still work?**
# MAGIC
# MAGIC If the current user is also the **owner** of `mask_ssn`, the output still shows `ALL PRIVILEGES` and `MANAGE` for the current user as the owner, so Unity Catalog continues to allow this user to run `mask_ssn` even after EXECUTE is revoked. In a real multi‑user workspace, revoking EXECUTE from a different (non‑owner) user or group (who does not have these owner-level privileges) would actually prevent them from calling the function until you grant EXECUTE again.

# COMMAND ----------

# MAGIC %md
# MAGIC #### G4(C). Granting EXECUTE on mask_ssn
# MAGIC Now grant `EXECUTE` on `mask_ssn` back to the same user.

# COMMAND ----------

spark.sql(
    f"GRANT EXECUTE ON FUNCTION `{CONFIG['abac_catalog_name']}`.securedata.mask_ssn TO `{CONFIG['username']}`"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### G4(D). Verifying EXECUTE Privileges After Grant
# MAGIC Verify again that `EXECUTE` for this user now appears in the grants.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ensure you are in the correct catalog and schema before checking the grants
# MAGIC USE CATALOG IDENTIFIER(CONFIG['abac_catalog_name']);
# MAGIC USE SCHEMA securedata;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON FUNCTION securedata.mask_ssn;

# COMMAND ----------

# MAGIC %md
# MAGIC #### G4(E). Running mask_ssn
# MAGIC
# MAGIC Finally, call the `mask_ssn` directly in SQL to demonstrate that the function can be executed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   '123-45-6789' AS original_ssn,
# MAGIC   IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.mask_ssn')('123-45-6789') AS masked_ssn;

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Row and Column Security (Legacy Table-Level Controls)
# MAGIC
# MAGIC Row and column security uses table-attached row filters and column masks implemented as **SQL user-defined functions** on individual tables, providing fine-grained access control but requiring object-by-object configuration and maintenance, which Databricks now treats as a legacy mechanism relative to policy-based governance.

# COMMAND ----------

# MAGIC %md
# MAGIC ### H1. Column Masking on SSN
# MAGIC In this section, we reuse the existing **`mask_ssn`** and **`filter_by_salary`** functions on the `employees` table to demonstrate legacy, table-scoped row and column security without ABAC.

# COMMAND ----------

# MAGIC %md
# MAGIC #### H1(A). Query the Table before Masking
# MAGIC
# MAGIC Let us analyze the **`employees`** table before applying a column mask.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.employees')
# MAGIC ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC #### H1(B). Attach the Existing `mask_ssn` Function as a Column Mask
# MAGIC
# MAGIC Let us alter the **`employees`** table to apply the existing **`mask_ssn`** function to redact the `ssn` column.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG IDENTIFIER(CONFIG['abac_catalog_name']);
# MAGIC USE SCHEMA securedata;
# MAGIC
# MAGIC ALTER TABLE securedata.employees
# MAGIC ALTER COLUMN ssn
# MAGIC SET MASK securedata.mask_ssn;

# COMMAND ----------

# MAGIC %md
# MAGIC #### H1(C). Query the Table with Column Masking
# MAGIC
# MAGIC Let us analyze the **`employees`** table after applying the column mask to verify that `ssn` values are redacted for non-privileged users.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.employees')
# MAGIC ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC #### H1(D). Drop the Column Mask
# MAGIC
# MAGIC Let us alter the **`employees`** table to drop the column mask on `ssn`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE securedata.employees
# MAGIC ALTER COLUMN ssn
# MAGIC DROP MASK;

# COMMAND ----------

# MAGIC %md
# MAGIC #### H1(E). Query the Table after Removing the Mask
# MAGIC
# MAGIC Let us analyze the **`employees`** table after removing the column mask to confirm that `ssn` is fully visible again.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.employees')
# MAGIC ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### H2. Row Filtering on Salary
# MAGIC
# MAGIC Let us now implement row filtering on the **`employees`** table using the existing **`filter_by_salary`** function and analyze the results.

# COMMAND ----------

# MAGIC %md
# MAGIC #### H2(A). Query the Table before Row Filtering
# MAGIC
# MAGIC Let us analyze the **`employees`** table before applying a row filter.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.employees')
# MAGIC ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC #### H2(B). Attach the Existing `filter_by_salary` Function as a Row Filter
# MAGIC
# MAGIC Let us alter the **`employees`** table to apply the **`filter_by_salary`** function as a row filter on the `salary` column.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG IDENTIFIER(CONFIG['abac_catalog_name']);
# MAGIC USE SCHEMA securedata;
# MAGIC
# MAGIC ALTER TABLE securedata.employees
# MAGIC SET ROW FILTER securedata.filter_by_salary ON (salary);

# COMMAND ----------

# MAGIC %md
# MAGIC #### H2(C). Query the Table with Row Filtering
# MAGIC
# MAGIC Let us analyze the **`employees`** table after applying the row filter to verify that only allowed salary rows are returned for non-privileged users.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.employees')
# MAGIC ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC #### H2(D). Drop the Row Filter
# MAGIC
# MAGIC Let us alter the **`employees`** table to drop the row filter.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE securedata.employees DROP ROW FILTER;

# COMMAND ----------

# MAGIC %md
# MAGIC #### H2(E). Query the Table after Removing the Row Filter
# MAGIC
# MAGIC Let us analyze the **`employees`** table after removing the row filter to confirm that all rows are visible again.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.employees')
# MAGIC ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## I. Protecting Columns and Rows with Dynamic Views (Legacy View-Based Controls)
# MAGIC
# MAGIC Dynamic views implement fine-grained row and column security at the view layer by embedding predicates and masking logic using functions such as `current_user()` and `is_account_group_member()`, but they are also considered a legacy pattern because access logic is tightly coupled to each view definition rather than centrally governed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### I1. Redacting Columns via Dynamic View
# MAGIC
# MAGIC Suppose we want a consumer to see employee records but with SSNs always redacted, while salaries remain visible only to privileged users.

# COMMAND ----------

# MAGIC %md
# MAGIC #### I1(A). Re-Create the Dynamic View Using Existing Functions
# MAGIC
# MAGIC Run the following cell to (re)define a **`secure_employees`** view that uses **`mask_ssn`** for the `ssn` column and an inline condition for salary redaction.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.secure_employees') AS
# MAGIC SELECT
# MAGIC   employee_id,
# MAGIC   name,
# MAGIC   securedata.mask_ssn(ssn) AS ssn,
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('Users') THEN salary
# MAGIC     ELSE NULL
# MAGIC   END AS salary,
# MAGIC   email,
# MAGIC   region
# MAGIC FROM securedata.employees;

# COMMAND ----------

# MAGIC %md
# MAGIC #### I1(B). Grant Access on the View to a User
# MAGIC
# MAGIC Run the following cell to grant **SELECT** on the **`secure_employees`** view to the username saced in `CONFIG["username"]` variable (replace with your actual target username).

# COMMAND ----------

spark.sql(f"GRANT SELECT ON VIEW securedata.secure_employees TO `{CONFIG['username']}`")

# COMMAND ----------

# MAGIC %md
# MAGIC #### I1(C). Query the View
# MAGIC
# MAGIC Let us query the **`secure_employees`** view as the owner.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM securedata.secure_employees
# MAGIC ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC When queried, the `ssn` will always appear masked, and salary visibility will follow the group-based logic in the view definition.

# COMMAND ----------

# MAGIC %md
# MAGIC ### I2. Restricting Rows via Dynamic View
# MAGIC
# MAGIC Now suppose we want a dynamic view that returns only a subset of rows, for example limiting exposure based on salary using our existing filter function.

# COMMAND ----------

# MAGIC %md
# MAGIC #### I2(A). Re-Create the View with a Row Predicate
# MAGIC
# MAGIC Run the following cell to recreate **`secure_employees`** with an additional row predicate using **`filter_by_salary`**.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.secure_employees') AS
# MAGIC SELECT
# MAGIC   employee_id,
# MAGIC   name,
# MAGIC   securedata.mask_ssn(ssn) AS ssn,
# MAGIC   salary,
# MAGIC   email,
# MAGIC   region
# MAGIC FROM securedata.employees
# MAGIC WHERE securedata.filter_by_salary(salary);

# COMMAND ----------

# MAGIC %md
# MAGIC #### I2(B). Re-Issue Grant to a User
# MAGIC
# MAGIC If needed, re-issue the **SELECT** grant on the view to ensure the username in the `CONFIG["username"]` variable (replace with your actual target username) can still query it.

# COMMAND ----------

spark.sql(f"GRANT SELECT ON VIEW securedata.secure_employees TO `{CONFIG['username']}`")

# COMMAND ----------

# MAGIC %md
# MAGIC #### I2(C). Query the View
# MAGIC
# MAGIC Let us query the **`secure_employees`** view again.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM securedata.secure_employees
# MAGIC ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC When executed, only rows allowed by **`filter_by_salary`** will be returned, combining legacy row restriction and column masking entirely at the view layer, without any governed tags or ABAC policies.

# COMMAND ----------

# MAGIC %md
# MAGIC ### I3. Attribute-Based Access Control with Governed Tags
# MAGIC
# MAGIC **Attribute-based access control with governed tags** is the current, recommended model in Unity Catalog, where account-level governed tags and ABAC policies, combined with **user-defined functions**, drive dynamic masking and row filtering that inherit across catalogs, schemas, tables, and columns, enabling centralized, non-overridable enforcement instead of per-object logic.
# MAGIC
# MAGIC **How ABAC with Governed Tags Works?**
# MAGIC
# MAGIC ABAC uses account-level **governed tags** (for example, `pii = ssn`, `classification = salary_masked`) attached to catalogs, schemas, tables, and columns, together with centrally managed **policies** that invoke UDFs such as `mask_ssn` and `filter_by_salary`. Instead of assigning masks and filters per table, you declare a single policy that automatically applies to any object whose tags match the policy's conditions.
# MAGIC
# MAGIC At a high level:
# MAGIC - You assign governed tags to `securedata.employees` and to sensitive columns like `ssn`, `email`, and `salary`.
# MAGIC - You reference existing UDFs (for example, `securedata.mask_ssn`, `securedata.filter_by_salary`) inside ABAC policies defined on the `securedata` schema.
# MAGIC - Unity Catalog enforces these policies at query time for all tagged objects, without requiring view rewrites or per-table ALTER statements.

# COMMAND ----------

# MAGIC %md
# MAGIC #### I3(A). Query the Employees Table Before Implementing ABAC
# MAGIC Run the following cell to query the `employees` table:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.employees') ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC #### I3(B). Defining a Column Mask Policy Using `mask_ssn`
# MAGIC
# MAGIC Let us now create an ABAC **column mask** policy that uses the existing **`mask_ssn`** function for any column tagged as `pii = ssn` under the `securedata` schema.
# MAGIC
# MAGIC Run the below code to create this policy:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set default catalog to CONFIG['abac_catalog_name'] and schema to securedata
# MAGIC USE CATALOG IDENTIFIER(CONFIG['abac_catalog_name']);
# MAGIC USE SCHEMA securedata;
# MAGIC
# MAGIC -- Create a column mask ABAC policy for masking the ssn column based on the pii:ssn governed tag
# MAGIC CREATE OR REPLACE POLICY MaskSSNPolicy
# MAGIC ON SCHEMA securedata
# MAGIC COMMENT 'Masks SSN for non-compliant users'
# MAGIC COLUMN MASK securedata.mask_ssn
# MAGIC TO `account users`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS
# MAGIC     hasTagValue('pii', 'ssn') AS ssn
# MAGIC ON COLUMN ssn;

# COMMAND ----------

# MAGIC %md
# MAGIC Once this policy is in place, any column in `securedata` that carries the governed tag `pii = ssn` will be masked using `mask_ssn` for all account users, regardless of the specific table.

# COMMAND ----------

# MAGIC %md
# MAGIC #### I3(C). Query the Employees Table After Implementing Comlumn Masking via ABAC
# MAGIC Run the following cell to query the `employees` table:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.employees') ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC #### I3(D). Defining a Row Filter Policy Using `filter_by_salary`
# MAGIC
# MAGIC Run the following cell to create an ABAC **row filter** policy that reuses the existing **`filter_by_salary`** function for any salary column tagged with `classification = salary_masked`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set default catalog to CONFIG['abac_catalog_name'] and schema to securedata
# MAGIC USE CATALOG IDENTIFIER(CONFIG['abac_catalog_name']);
# MAGIC USE SCHEMA securedata;
# MAGIC
# MAGIC -- Create a row filter ABAC policy for filtering the rows column based on the classification:salary_masked governed tag
# MAGIC CREATE OR REPLACE POLICY SalaryRowPolicy
# MAGIC ON SCHEMA securedata
# MAGIC COMMENT 'Restrict high-salary rows for non-privileged users'
# MAGIC ROW FILTER securedata.filter_by_salary
# MAGIC TO `account users`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS
# MAGIC   hasTagValue('classification', 'salary_masked') AS masked_salary
# MAGIC USING COLUMNS (masked_salary);

# COMMAND ----------

# MAGIC %md
# MAGIC With this policy, any table in `securedata` that has a `salary` column tagged with `classification = salary_masked` will automatically be filtered by `filter_by_salary` for all account users, without additional table-level configuration.

# COMMAND ----------

# MAGIC %md
# MAGIC #### I3(E). Query the Employees Table After Implementing Row Filtering via ABAC
# MAGIC Run the following cell to query the `employees` table:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.employees') ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC You will notice that masking and filtering are now driven by governed tags and central policies rather than per-table or per-view logic, providing consistent, scalable enforcement across your Unity Catalog environment.

# COMMAND ----------

# MAGIC %md
# MAGIC #### I3(F). Removing the ABAC Policies
# MAGIC
# MAGIC Let us now remove the ABAC policies that we created on the `securedata` schema.
# MAGIC
# MAGIC Run the following cells to remove the ABAC policies from the **securedata** schema:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set default catalog to CONFIG['abac_catalog_name'] and schema to securedata
# MAGIC USE CATALOG IDENTIFIER(CONFIG['abac_catalog_name']);
# MAGIC USE SCHEMA securedata;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete the row filter policy SalaryRowPolicy from the securedata schema
# MAGIC DROP POLICY SalaryRowPolicy ON SCHEMA securedata;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete the column mask policy MaskSSNPolicy from the securedata schema
# MAGIC DROP POLICY MaskSSNPolicy ON SCHEMA securedata;

# COMMAND ----------

# MAGIC %md
# MAGIC #### I3(G). Validating ABAC Removal
# MAGIC Run the following cell to confirm that masking and filtering are no longer enforced by ABAC:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.employees') ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Examining the output, you will notice that all rows and column values are visible again without ABAC-based masking or row filtering, confirming that access restrictions were entirely dependent on the ABAC policies you just removed.

# COMMAND ----------

# MAGIC %md
# MAGIC #### I3(H). Policy Inheritance, Enforcement, and Validation

# COMMAND ----------

# MAGIC %md
# MAGIC **ABAC Inheritance Behavior**
# MAGIC
# MAGIC - Catalog- or schema-level ABAC policies and governed tags automatically cascade to child tables and columns unless explicitly overridden by a more specific policy.  
# MAGIC - Only one active row filter and one active column mask can apply per table/column at a time, whether configured via ABAC or directly.  
# MAGIC - Views themselves do not support ABAC policies; ABAC is evaluated on the underlying Unity Catalog tables and columns.

# COMMAND ----------

# MAGIC %md
# MAGIC **Enforcement Validation (SQL)**
# MAGIC
# MAGIC Run these validation queries to confirm that ABAC masking and filtering are being applied as expected:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validate SSN masking (should return redacted SSN if tag and policy matched)
# MAGIC SELECT employee_id, name, email, region, IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.mask_ssn') (ssn) as masked_ssn
# MAGIC FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.employees');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validate row filtering (only lower-salary rows for non-privileged users; all rows for you/admins)
# MAGIC SELECT *
# MAGIC FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.employees')
# MAGIC WHERE IDENTIFIER(CONFIG['abac_catalog_name'] || '.securedata.filter_by_salary') (salary);

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:**
# MAGIC - The lab environment uses a single, non-admin user, so you won't see different ABAC behaviors here.  
# MAGIC - In a personal or organizational workspace (with proper roles/privileges), you can rerun these queries as different users (regular vs. admin/privileged) to see how ABAC changes visible rows and values.

# COMMAND ----------

# MAGIC %md
# MAGIC **ABAC Limitations**
# MAGIC
# MAGIC - Only a single row filter and a single column mask can be active per table/column, which means ABAC and manual table-level filters/masks cannot stack on the same target.  
# MAGIC - ABAC policies govern Unity Catalog objects (catalogs, schemas, tables, and columns) and are not applied directly to views, though views still reflect the filtered/masked results of underlying tables.  
# MAGIC - Policies are evaluated centrally for all attached workspaces that share the same metastore, so changes affect all consumers of tagged data in that metastore.  
# MAGIC - Compute must support Unity Catalog ABAC enforcement; older runtimes or non-UC compute will not evaluate governed tags or ABAC policies.

# COMMAND ----------

# MAGIC %md
# MAGIC ## J. Clean up
# MAGIC Run the below cell to drop the user-created `db_abac_{your_username}` catalog \(and all of its contents\) from the metastore:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 🗑️ Drop the user-created `CONFIG['abac_catalog_name']` catalog and all its contents from the metastore.
# MAGIC DROP CATALOG IF EXISTS IDENTIFIER(CONFIG['abac_catalog_name']) CASCADE;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Conclusion
# MAGIC
# MAGIC You have successfully implemented attribute-based access control (ABAC) in Unity Catalog using governed tags, reusable UDFs, and centralized policies to enforce fine-grained row and column security for sensitive HR data. This demo connected legacy mechanisms—manual row filters, column masks, and dynamic views—with the recommended ABAC model so you can reason about when and how to modernize existing controls.
# MAGIC
# MAGIC Key takeaways from this demo include:
# MAGIC
# MAGIC - **Governed tags** classify data and drive policy-based, tag-aware governance across Unity Catalog objects.
# MAGIC - **Row filters and column masks** apply UDF-based row and column security directly at the table level.
# MAGIC - **Dynamic views** enforce row and column security at the view layer using user- and group-aware predicates.
# MAGIC - **ABAC policies** centralize masking and filtering rules, inheriting from higher scopes and complementing GRANT/REVOKE privileges.
# MAGIC - **Privilege management** (GRANT/REVOKE) remains the foundation for who can access and manage governed data assets.
# MAGIC
# MAGIC With hands-on experience in assigning governed tags, creating UDFs, attaching row filters and column masks, and defining ABAC policies, you are equipped to design compliance-ready, scalable security architectures that unify legacy controls and modern, tag-driven governance across your Databricks Lakehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>