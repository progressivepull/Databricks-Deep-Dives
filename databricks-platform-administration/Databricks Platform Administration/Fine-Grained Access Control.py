# Databricks notebook source (7.1 Lab - Implementing Fine-Grained Access Control for Global Financial Services)
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
# MAGIC # Lab - Implementing Fine-Grained Access Control for Global Financial Services
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lab, you will step into the role of a **Data Security Engineer** at GlobalFinance Corp, a multinational financial services company that processes sensitive customer data across multiple regions. Your organization must prove that it can protect personally identifiable information (PII) and sensitive financial data while still enabling analysts, managers, and customer-facing teams to do their jobs effectively.
# MAGIC
# MAGIC You will design and implement a fine-grained access control framework using Unity Catalog's attribute-based access control (ABAC) capabilities. Starting from raw customer accounts and transaction data, you will classify sensitive fields with governed tags, build reusable masking and filtering functions, and configure centralized ABAC policies that automatically enforce masking and row filtering based on user roles, risk levels, and regulatory requirements.
# MAGIC
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this demo, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Design and implement a comprehensive data governance structure using Unity Catalog catalogs, schemas, and governed tags</li>
# MAGIC         <li>Create reusable user-defined functions for column masking and row filtering that support business compliance requirements</li>
# MAGIC         <li>Build and deploy attribute-based access control (ABAC) policies that automatically enforce fine-grained security based on data classification tags</li>
# MAGIC         <li>Validate and test access control policies across different user scenarios and regional compliance requirements</li>
# MAGIC         <li>Compare legacy table-level controls with modern centralized ABAC governance for scalable data protection</li>
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
# MAGIC
# MAGIC ## Conventions
# MAGIC
# MAGIC This notebook uses standard helper objects (such as `CONFIG`) for data paths and display  
# MAGIC
# MAGIC **Lab Structure**
# MAGIC - **TODO** cells require you to implement missing code using `<FILL_IN>` placeholders  
# MAGIC - **ANSWER** cells provide solutions that you can reveal after attempting the exercises  
# MAGIC - Use dropdown sections to hide or show answer cells as needed
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this lab.

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

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A2. Initialize GlobalFinance Governance Environment
# MAGIC
# MAGIC **Business Context:** GlobalFinance Corp's lakehouse environment uses a standardized configuration pattern to ensure that every engineer works in an isolated, governance-ready sandbox. A shared `CONFIG` object is created during classroom setup so that all catalogs, schemas, tables, and policies you build are unique to your user while still following corporate naming and compliance conventions.
# MAGIC
# MAGIC **Setup includes:**
# MAGIC
# MAGIC 1. **Creates a `CONFIG` object** as both a Python dictionary and SQL Map Variable to store key variables for this lab (such as `username`, `catalog_name`, and `abac_catalog_name`).
# MAGIC 2. **Generates a unique base catalog name** for each user based on their username (for example, `db_{your_username}`).
# MAGIC 3. **Generates a dedicated ABAC catalog name** for financial governance scenarios (for example, `db_abac_{your_username}`, exposed as `CONFIG['abac_catalog_name']`).
# MAGIC 4. **Sanitizes all names** to ensure they are valid Unity Catalog identifiers, handling special characters, case rules, and length limits.
# MAGIC 5. **Captures environment metadata**, including the current user and creation timestamp, to support auditability and repeatable runs across different workspaces.
# MAGIC
# MAGIC **Structure Created**
# MAGIC
# MAGIC **1. Catalogs:**
# MAGIC
# MAGIC - `db_abac_{your_username}` – Primary catalog for GlobalFinance Corp customer financial data and ABAC policies (represented by `CONFIG['abac_catalog_name']`).
# MAGIC
# MAGIC **2. Schemas & Assets:**
# MAGIC
# MAGIC <table style="width: 100%; border-collapse: collapse; border: 1px solid #ddd;">
# MAGIC   <tr style="background-color: #f2f2f2;">
# MAGIC     <th style="border: 1px solid #ddd; padding: 12px; text-align: left;"><strong>Component</strong></th>
# MAGIC     <th style="border: 1px solid #ddd; padding: 12px; text-align: left;"><strong>Location</strong></th>
# MAGIC     <th style="border: 1px solid #ddd; padding: 12px; text-align: left;"><strong>Contents</strong></th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="border: 1px solid #ddd; padding: 12px;"><strong>Schema</strong></td>
# MAGIC     <td style="border: 1px solid #ddd; padding: 12px;"><code>db_abac_{your_username}.customerdata</code></td>
# MAGIC     <td style="border: 1px solid #ddd; padding: 12px;">Sensitive customer financial data subject to GDPR, CCPA, and banking regulations</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="border: 1px solid #ddd; padding: 12px;"><strong>Tables</strong></td>
# MAGIC     <td style="border: 1px solid #ddd; padding: 12px;"><code>db_abac_{your_username}.customerdata</code></td>
# MAGIC     <td style="border: 1px solid #ddd; padding: 12px;">
# MAGIC       <code>customeraccounts</code> (PII + account profile), 
# MAGIC       <code>customertransactions</code> (transaction history and risk scores)
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="border: 1px solid #ddd; padding: 12px;"><strong>UDFs</strong></td>
# MAGIC     <td style="border: 1px solid #ddd; padding: 12px;"><code>db_abac_{your_username}.customerdata</code></td>
# MAGIC     <td style="border: 1px solid #ddd; padding: 12px;">
# MAGIC       <code>maskcustomerssn</code>, 
# MAGIC       <code>maskcustomeremail</code>, 
# MAGIC       <code>maskaccountbalance</code>, 
# MAGIC       <code>filterbyriskcategory</code>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="border: 1px solid #ddd; padding: 12px;"><strong>ABAC Policies</strong></td>
# MAGIC     <td style="border: 1px solid #ddd; padding: 12px;"><code>db_abac_{your_username}.customerdata</code></td>
# MAGIC     <td style="border: 1px solid #ddd; padding: 12px;">
# MAGIC       <code>GlobalFinanceSSNMaskingPolicy</code>, 
# MAGIC       <code>GlobalFinanceEmailMaskingPolicy</code>, 
# MAGIC       <code>GlobalFinanceBalanceMaskingPolicy</code>, 
# MAGIC       <code>GlobalFinanceRiskFilterPolicy</code>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC
# MAGIC **3. Hierarchical Layout**
# MAGIC
# MAGIC <pre style="background-color: #f5f5f5; border: 1px solid #ddd; border-radius: 4px; padding: 12px; font-size: 14px; line-height: 1.6; overflow-x: auto;">
# MAGIC [db_abac_{your_username}]  (CONFIG['abac_catalog_name'])
# MAGIC └── [customer_data]
# MAGIC     ├── Tables
# MAGIC     │   ├── customeraccounts        (PII + financial attributes)
# MAGIC     │   └── customertransactions    (regional activity and risk signals)
# MAGIC     ├── UDFs
# MAGIC     │   ├── maskcustomerssn
# MAGIC     │   ├── maskcustomeremail
# MAGIC     │   ├── maskaccountbalance
# MAGIC     │   └── filterbyriskcategory
# MAGIC     └── ABAC Policies
# MAGIC         ├── GlobalFinanceSSNMaskingPolicy
# MAGIC         ├── GlobalFinanceEmailMaskingPolicy
# MAGIC         ├── GlobalFinanceBalanceMaskingPolicy
# MAGIC         └── GlobalFinanceRiskFilterPolicy
# MAGIC </pre>
# MAGIC
# MAGIC **Note:** The classroom setup notebook (`./Includes/Classroom-Setup-1`) constructs the `CONFIG` object and prepares your isolated catalog and schema. In this lab, you will populate these structures with tables, tags, UDFs, and policies to implement end-to-end, tag-driven financial data governance.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to configure the key environment variables used throughout this lab:

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-1

# COMMAND ----------

# MAGIC %md
# MAGIC ### A3. Inspect Lab Configuration
# MAGIC Run the code blocks below to verify the specific catalog names and user details generated for your current environment:

# COMMAND ----------

## Display the username, default catalog name, and ABAC catalog name
## Use the CONFIG dictionary to access these values
print(f"GlobalFinance User:     <FILL_IN>")
print(f"Base Catalog:           <FILL_IN>")
print(f"Financial ABAC Catalog: <FILL_IN>")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the Python answer -->
# MAGIC <textarea id="raw-answer-python-display-config-all-catalogs" style="display:none;">
# MAGIC %python
# MAGIC # ANSWER
# MAGIC # Display the username, default catalog name, and ABAC catalog name
# MAGIC # Use the CONFIG dictionary to access these values
# MAGIC print(f"GlobalFinance User:     {CONFIG['username']}")
# MAGIC print(f"Base Catalog:           {CONFIG['catalog_name']}")
# MAGIC print(f"Financial ABAC Catalog: {CONFIG['abac_catalog_name']}")
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="python" data-source="raw-answer-python-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-okaidia.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-python.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         // Force python as the default here, since this block is Python code.
# MAGIC         var lang = block.getAttribute('data-language') || 'python';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load Python from hidden textarea and render inside <pre><code class="language-python"> for Prism [web:4][web:159][web:173].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#272822;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code; // Prism highlights <pre><code class="language-python"> blocks [web:14][web:159]
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Use SQL to display the username, default catalog name, and ABAC catalog name
# MAGIC ---- Uses the STACK function to format CONFIG values as a table with Property Type and Value columns
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Use SQL to display the username, default catalog name, and ABAC catalog name
# MAGIC -- Uses the STACK function to format CONFIG values as a table with Property Type and Value columns
# MAGIC SELECT stack(4,
# MAGIC   'GlobalFinance User:',      CONFIG['username'],
# MAGIC   'Base Catalog',             CONFIG['catalog_name'],
# MAGIC   'Financial ABAC Catalog',   CONFIG['abac_catalog_name']
# MAGIC ) AS (`Property Type`, Value);
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Create GlobalFinance Corp Data Architecture
# MAGIC
# MAGIC Build the foundational data architecture for GlobalFinance Corp including regional catalogs, compliance schemas, and customer financial tables with appropriate governance tags.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Understand Regional Compliance Requirements
# MAGIC
# MAGIC GlobalFinance Corp must comply with different regional regulations:
# MAGIC - **GDPR (EMEA)**: Strict PII protection, right to be forgotten, data minimization
# MAGIC - **CCPA (Americas)**: Consumer privacy rights, opt-out mechanisms
# MAGIC - **Banking Regulations (Asia Pacific)**: Financial data segregation, audit trails
# MAGIC
# MAGIC Run the following cell to check your current group memberships which will determine your access level:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check current group memberships for GlobalFinance Corp role-based access
# MAGIC SELECT
# MAGIC   is_account_group_member('users') as is_analyst_role,
# MAGIC   is_account_group_member('admins') as is_compliance_officer_role,
# MAGIC   current_user() as globalfinance_user_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Create Financial Services Catalog
# MAGIC
# MAGIC Create the main catalog for GlobalFinance Corp's financial data with appropriate governance tags.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create the financial services catalog using the ABAC catalog name from CONFIG
# MAGIC ---- Add a comment describing this as GlobalFinance Corp's main financial data catalog
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Create the financial services catalog using the ABAC catalog name from CONFIG
# MAGIC -- Add a comment describing this as GlobalFinance Corp's main financial data catalog
# MAGIC CREATE CATALOG IF NOT EXISTS IDENTIFIER(CONFIG['abac_catalog_name'])
# MAGIC COMMENT 'GlobalFinance Corp main catalog for customer financial data and regulatory compliance';
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the current catalog for subsequent operations
# MAGIC USE CATALOG IDENTIFIER(CONFIG['abac_catalog_name']);

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. Apply Governance Tags to Catalog
# MAGIC
# MAGIC Now, apply business and compliance tags to the GlobalFinance Corp catalog.

# COMMAND ----------

## Apply the following tags to the catalog:
## - department: "financial_services"
## - env: "production" 
## - owner: "data_governance_team"
## - compliance_scope: "global_financial_regulations"
## - business_unit: "globalfinance_corp"

## Define the tag values for our financial services context
desired_tags = {
    <FILL_IN>: <FILL_IN>,
    <FILL_IN>: <FILL_IN>,
    <FILL_IN>: <FILL_IN>,
    <FILL_IN>: <FILL_IN>,
    <FILL_IN>: <FILL_IN>
}

## Fetch existing tags and unset them if they exist
rows = spark.sql(f"""
  SELECT tag_name, tag_value
  FROM information_schema.catalog_tags
  WHERE catalog_name = '{CONFIG["abac_catalog_name"]}'
""").collect()
existing = {r["tag_name"]: r["tag_value"] for r in rows}

## Unset existing tags to avoid conflicts
for key in desired_tags.keys():
    if key in existing:
        spark.sql(f"UNSET TAG ON CATALOG {CONFIG['abac_catalog_name']} `{key}`")

## Set the new governance tags
for key, value in desired_tags.items():
    spark.sql(f"SET TAG ON CATALOG {CONFIG['abac_catalog_name']} `{key}` = `{value}`")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the Python answer -->
# MAGIC <textarea id="raw-answer-python-display-config-all-catalogs" style="display:none;">
# MAGIC %python
# MAGIC # ANSWER
# MAGIC # Apply the following tags to the catalog:
# MAGIC # - department: "financial_services"
# MAGIC # - env: "production" 
# MAGIC # - owner: "data_governance_team"
# MAGIC # - compliance_scope: "global_financial_regulations"
# MAGIC # - business_unit: "globalfinance_corp"
# MAGIC
# MAGIC # Define the tag values for our financial services context
# MAGIC desired_tags = {
# MAGIC     "department": "financial_services",
# MAGIC     "env": "production",
# MAGIC     "owner": "data_governance_team", 
# MAGIC     "compliance_scope": "global_financial_regulations",
# MAGIC     "business_unit": "globalfinance_corp"
# MAGIC }
# MAGIC
# MAGIC # Fetch existing tags and unset them if they exist
# MAGIC rows = spark.sql(f"""
# MAGIC   SELECT tag_name, tag_value
# MAGIC   FROM information_schema.catalog_tags
# MAGIC   WHERE catalog_name = '{CONFIG["abac_catalog_name"]}'
# MAGIC """).collect()
# MAGIC existing = {r["tag_name"]: r["tag_value"] for r in rows}
# MAGIC
# MAGIC # Unset existing tags to avoid conflicts
# MAGIC for key in desired_tags.keys():
# MAGIC     if key in existing:
# MAGIC         spark.sql(f"UNSET TAG ON CATALOG {CONFIG['abac_catalog_name']} `{key}`")
# MAGIC
# MAGIC # Set the new governance tags
# MAGIC for key, value in desired_tags.items():
# MAGIC     spark.sql(f"SET TAG ON CATALOG {CONFIG['abac_catalog_name']} `{key}` = `{value}`")
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="python" data-source="raw-answer-python-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-okaidia.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-python.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         // Force python as the default here, since this block is Python code.
# MAGIC         var lang = block.getAttribute('data-language') || 'python';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load Python from hidden textarea and render inside <pre><code class="language-python"> for Prism [web:4][web:159][web:173].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#272822;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code; // Prism highlights <pre><code class="language-python"> blocks [web:14][web:159]
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### B4. Create Customer Data Schema
# MAGIC
# MAGIC Create a schema specifically for sensitive customer financial data.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create a schema called 'customer_data' for sensitive customer financial information
# MAGIC ---- Include a comment explaining this schema contains PII and financial data subject to regulations
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Create a schema called 'customer_data' for sensitive customer financial information
# MAGIC -- Include a comment explaining this schema contains PII and financial data subject to regulations
# MAGIC CREATE SCHEMA IF NOT EXISTS customer_data 
# MAGIC COMMENT 'Schema for sensitive customer financial data subject to GDPR, CCPA, and banking regulations';
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the customer_data schema as the current schema
# MAGIC USE SCHEMA customer_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ### B5. Apply Compliance Tags to Schema
# MAGIC
# MAGIC Tag the customer_data schema with appropriate compliance and sensitivity classifications.

# COMMAND ----------

## Apply these tags to the customer_data schema:
## - sensitivity: "high"
## - compliance_framework: "gdpr_ccpa_banking"
## - data_classification: "customer_pii_financial"
## - retention_policy: "7_years"
## - audit_required: "true"

## Define the tag values
desired_schema_tags = {
    <FILL_IN>: <FILL_IN>,
    <FILL_IN>: <FILL_IN>, 
    <FILL_IN>: <FILL_IN>,
    <FILL_IN>: <FILL_IN>,
    <FILL_IN>: <FILL_IN>
}

## Fetch existing schema tags
rows = spark.sql("""
  SELECT tag_name, tag_value
  FROM information_schema.schema_tags
  WHERE schema_name = 'customer_data'
""").collect()
existing = {r["tag_name"]: r["tag_value"] for r in rows}

## Unset existing tags
for key in desired_schema_tags.keys():
    if key in existing:
        spark.sql(f"UNSET TAG ON SCHEMA customer_data `{key}`")

## Set new tags
for key, value in desired_schema_tags.items():
    spark.sql(f"SET TAG ON SCHEMA customer_data `{key}` = `{value}`")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the Python answer -->
# MAGIC <textarea id="raw-answer-python-display-config-all-catalogs" style="display:none;">
# MAGIC %python
# MAGIC # ANSWER
# MAGIC # Apply these tags to the customer_data schema:
# MAGIC # - sensitivity: "high"
# MAGIC # - compliance_framework: "gdpr_ccpa_banking"
# MAGIC # - data_classification: "customer_pii_financial"
# MAGIC # - retention_policy: "7_years"
# MAGIC # - audit_required: "true"
# MAGIC
# MAGIC # Define the tag values
# MAGIC desired_schema_tags = {
# MAGIC     "sensitivity": "high",
# MAGIC     "compliance_framework": "gdpr_ccpa_banking",
# MAGIC     "data_classification": "customer_pii_financial",
# MAGIC     "retention_policy": "7_years",
# MAGIC     "audit_required": "true"
# MAGIC }
# MAGIC
# MAGIC # Fetch existing schema tags
# MAGIC rows = spark.sql("""
# MAGIC   SELECT tag_name, tag_value
# MAGIC   FROM information_schema.schema_tags
# MAGIC   WHERE schema_name = 'customer_data'
# MAGIC """).collect()
# MAGIC existing = {r["tag_name"]: r["tag_value"] for r in rows}
# MAGIC
# MAGIC # Unset existing tags
# MAGIC for key in desired_schema_tags.keys():
# MAGIC     if key in existing:
# MAGIC         spark.sql(f"UNSET TAG ON SCHEMA customer_data `{key}`")
# MAGIC
# MAGIC # Set new tags
# MAGIC for key, value in desired_schema_tags.items():
# MAGIC     spark.sql(f"SET TAG ON SCHEMA customer_data `{key}` = `{value}`")
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="python" data-source="raw-answer-python-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-okaidia.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-python.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         // Force python as the default here, since this block is Python code.
# MAGIC         var lang = block.getAttribute('data-language') || 'python';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load Python from hidden textarea and render inside <pre><code class="language-python"> for Prism [web:4][web:159][web:173].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#272822;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code; // Prism highlights <pre><code class="language-python"> blocks [web:14][web:159]
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Create Customer Financial Data Tables
# MAGIC
# MAGIC Build realistic customer financial tables that represent GlobalFinance Corp's core data assets requiring fine-grained access control.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Create Customer Accounts Table
# MAGIC
# MAGIC Create a comprehensive customer accounts table with PII and financial data.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create a customer_accounts table with the following columns:
# MAGIC ---- - customer_id (INT)
# MAGIC ---- - first_name (STRING) 
# MAGIC ---- - last_name (STRING)
# MAGIC ---- - email (STRING)
# MAGIC ---- - phone (STRING)
# MAGIC ---- - ssn (STRING)
# MAGIC ---- - account_balance (DECIMAL(15,2))
# MAGIC ---- - credit_score (INT)
# MAGIC ---- - region (STRING)
# MAGIC ---- - account_type (STRING)
# MAGIC ---- - date_opened (DATE)
# MAGIC ---- - risk_category (STRING)
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Create a customer_accounts table with the following columns:
# MAGIC -- - customer_id (INT)
# MAGIC -- - first_name (STRING) 
# MAGIC -- - last_name (STRING)
# MAGIC -- - email (STRING)
# MAGIC -- - phone (STRING)
# MAGIC -- - ssn (STRING)
# MAGIC -- - account_balance (DECIMAL(15,2))
# MAGIC -- - credit_score (INT)
# MAGIC -- - region (STRING)
# MAGIC -- - account_type (STRING)
# MAGIC -- - date_opened (DATE)
# MAGIC -- - risk_category (STRING)
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS customer_accounts (
# MAGIC   customer_id INT,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   email STRING,
# MAGIC   phone STRING,
# MAGIC   ssn STRING,
# MAGIC   account_balance DECIMAL(15,2),
# MAGIC   credit_score INT,
# MAGIC   region STRING,
# MAGIC   account_type STRING,
# MAGIC   date_opened DATE,
# MAGIC   risk_category STRING
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Customer account master data with PII and financial information for GlobalFinance Corp';
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Create Customer Transactions Table
# MAGIC
# MAGIC Create a customer transactions table for transaction history and patterns.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create a customer_transactions table with the following columns:
# MAGIC ---- - transaction_id (STRING)
# MAGIC ---- - customer_id (INT) 
# MAGIC ---- - transaction_amount (DECIMAL(15,2))
# MAGIC ---- - transaction_type (STRING)
# MAGIC ---- - transaction_date (TIMESTAMP)
# MAGIC ---- - merchant_name (STRING)
# MAGIC ---- - region (STRING)
# MAGIC ---- - risk_score (DECIMAL(3,2))
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Create a customer_transactions table with the following columns:
# MAGIC -- - transaction_id (STRING)
# MAGIC -- - customer_id (INT) 
# MAGIC -- - transaction_amount (DECIMAL(15,2))
# MAGIC -- - transaction_type (STRING)
# MAGIC -- - transaction_date (TIMESTAMP)
# MAGIC -- - merchant_name (STRING)
# MAGIC -- - region (STRING)
# MAGIC -- - risk_score (DECIMAL(3,2))
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS customer_transactions (
# MAGIC   transaction_id STRING,
# MAGIC   customer_id INT,
# MAGIC   transaction_amount DECIMAL(15,2),
# MAGIC   transaction_type STRING,
# MAGIC   transaction_date TIMESTAMP,
# MAGIC   merchant_name STRING,
# MAGIC   region STRING,
# MAGIC   risk_score DECIMAL(3,2)
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Customer transaction history and risk analysis data';
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Apply Governed Tags for Data Classification
# MAGIC
# MAGIC Apply the pre-created governed tags (`pii` and `classification`) to classify sensitive columns for automatic ABAC policy enforcement.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Tag PII Columns in Customer Accounts
# MAGIC
# MAGIC Apply governed PII tags to personally identifiable information columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Apply the 'pii' governed tag to appropriate columns:
# MAGIC ---- - Tag the SSN column with 'pii' = 'ssn'
# MAGIC ---- - Tag the EMAIL column with 'pii' = 'email'  
# MAGIC ---- - Tag the PHONE column with 'pii' = 'address' (using address as closest available value)
# MAGIC ---- Also, apply the 'classification' tag to the 'risk_category' column:
# MAGIC ---- - Tag the RISK_CATEGORY column with 'classification' = 'salary_masked'
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Apply the 'pii' governed tag to appropriate columns:
# MAGIC
# MAGIC -- - Tag the SSN column with 'pii' = 'ssn'
# MAGIC ALTER TABLE customer_accounts ALTER COLUMN ssn SET TAGS ('pii' = 'ssn');
# MAGIC
# MAGIC -- - Tag the EMAIL column with 'pii' = 'email'
# MAGIC ALTER TABLE customer_accounts ALTER COLUMN email SET TAGS ('pii' = 'email');
# MAGIC
# MAGIC -- - Tag the PHONE column with 'pii' = 'address' (using address as closest available value)
# MAGIC ALTER TABLE customer_accounts ALTER COLUMN phone SET TAGS ('pii' = 'address');
# MAGIC
# MAGIC
# MAGIC -- Also, apply the 'classification' tag to financial columns:
# MAGIC
# MAGIC -- - Tag the RISK_CATEGORY column with 'classification' = 'salary_masked'
# MAGIC ALTER TABLE customer_accounts ALTER COLUMN risk_category SET TAGS ('classification' = 'salary_masked');
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Apply Regional and Business Tags
# MAGIC
# MAGIC Apply business metadata tags to both tables for regional compliance and business classification.

# COMMAND ----------

## Apply business tags to the customer_accounts table:
## - Set region tag to 'global' (since it contains data from all regions)
## - Set business_function to 'customer_management'
## - Set compliance_tier to 'tier_1_critical'

table_tags = {
    <FILL_IN>: <FILL_IN>,
    <FILL_IN>: <FILL_IN>,
    <FILL_IN>: <FILL_IN>
}

## Apply the tags using the same pattern as before
rows = spark.sql("""
  SELECT tag_name, tag_value
  FROM information_schema.table_tags
  WHERE table_name = 'customer_accounts'
""").collect()
existing = {r["tag_name"]: r["tag_value"] for r in rows}

for key in table_tags.keys():
    if key in existing:
        spark.sql(f"UNSET TAG ON TABLE customer_accounts `{key}`")

for key, value in table_tags.items():
    spark.sql(f"SET TAG ON TABLE customer_accounts `{key}` = `{value}`")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the Python answer -->
# MAGIC <textarea id="raw-answer-python-display-config-all-catalogs" style="display:none;">
# MAGIC %python
# MAGIC # ANSWER
# MAGIC # Apply business tags to the customer_accounts table:
# MAGIC # - Set region tag to 'global' (since it contains data from all regions)
# MAGIC # - Set business_function to 'customer_management'
# MAGIC # - Set compliance_tier to 'tier_1_critical'
# MAGIC
# MAGIC table_tags = {
# MAGIC     "region": "global",
# MAGIC     "business_function": "customer_management", 
# MAGIC     "compliance_tier": "tier_1_critical"
# MAGIC }
# MAGIC
# MAGIC # Apply the tags using the same pattern as before
# MAGIC rows = spark.sql("""
# MAGIC   SELECT tag_name, tag_value
# MAGIC   FROM information_schema.table_tags
# MAGIC   WHERE table_name = 'customer_accounts'
# MAGIC """).collect()
# MAGIC existing = {r["tag_name"]: r["tag_value"] for r in rows}
# MAGIC
# MAGIC for key in table_tags.keys():
# MAGIC     if key in existing:
# MAGIC         spark.sql(f"UNSET TAG ON TABLE customer_accounts `{key}`")
# MAGIC
# MAGIC for key, value in table_tags.items():
# MAGIC     spark.sql(f"SET TAG ON TABLE customer_accounts `{key}` = `{value}`")
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="python" data-source="raw-answer-python-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-okaidia.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-python.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         // Force python as the default here, since this block is Python code.
# MAGIC         var lang = block.getAttribute('data-language') || 'python';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load Python from hidden textarea and render inside <pre><code class="language-python"> for Prism [web:4][web:159][web:173].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#272822;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code; // Prism highlights <pre><code class="language-python"> blocks [web:14][web:159]
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Populate Tables with Realistic Financial Data
# MAGIC
# MAGIC Insert realistic customer financial data representing GlobalFinance Corp's customer base across different regions and risk profiles.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E1. Insert Customer Account Records
# MAGIC
# MAGIC Insert diverse customer account records representing different regions and risk profiles.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Insert customer account records with the following data:
# MAGIC ---- - Include customers from Americas, EMEA, and Asia Pacific regions
# MAGIC ---- - Mix of account types: checking, savings, investment, credit
# MAGIC ---- - Various risk categories: low, medium, high
# MAGIC ---- - Different account balances and credit scores
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Insert customer account records with the following data:
# MAGIC -- - Include customers from Americas, EMEA, and Asia Pacific regions
# MAGIC -- - Mix of account types: checking, savings, investment, credit
# MAGIC -- - Various risk categories: low, medium, high
# MAGIC -- - Different account balances and credit scores
# MAGIC
# MAGIC INSERT INTO customer_accounts VALUES
# MAGIC (1001, 'Sarah', 'Johnson', 'sarah.johnson@email.com', '+1-555-0101', '123-45-6789', 125000.50, 750, 'Americas', 'checking', '2020-01-15', 'low'),
# MAGIC (1002, 'Hans', 'Mueller', 'hans.mueller@email.com', '+49-30-12345', '987-65-4321', 89000.25, 680, 'EMEA', 'savings', '2019-06-22', 'medium'),
# MAGIC (1003, 'Yuki', 'Tanaka', 'yuki.tanaka@email.com', '+81-3-9876', '555-44-3333', 250000.00, 820, 'Asia Pacific', 'investment', '2021-03-10', 'low'),
# MAGIC (1004, 'Maria', 'Garcia', 'maria.garcia@email.com', '+34-91-5555', '222-33-4444', 45000.75, 620, 'EMEA', 'credit', '2022-08-14', 'high'),
# MAGIC (1005, 'David', 'Chen', 'david.chen@email.com', '+86-10-8888', '777-88-9999', 180000.00, 790, 'Asia Pacific', 'checking', '2020-11-05', 'low'),
# MAGIC (1006, 'Jennifer', 'Williams', 'jen.williams@email.com', '+1-555-0202', '111-22-3333', 95000.30, 710, 'Americas', 'savings', '2021-07-18', 'medium'),
# MAGIC (1007, 'Pierre', 'Dubois', 'pierre.dubois@email.com', '+33-1-4444', '666-77-8888', 320000.00, 850, 'EMEA', 'investment', '2018-12-03', 'low'),
# MAGIC (1008, 'Lisa', 'Anderson', 'lisa.anderson@email.com', '+1-555-0303', '444-55-6666', 15000.00, 580, 'Americas', 'credit', '2023-02-28', 'high');
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2. Insert Transaction Records
# MAGIC
# MAGIC Insert transaction records showing customer activity patterns across regions.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Insert transaction records for the customers above
# MAGIC ---- - Include various transaction types: purchase, transfer, deposit, withdrawal
# MAGIC ---- - Mix of amounts and risk scores
# MAGIC ---- - Distribute across different regions
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Insert transaction records for the customers above
# MAGIC -- - Include various transaction types: purchase, transfer, deposit, withdrawal
# MAGIC -- - Mix of amounts and risk scores
# MAGIC -- - Distribute across different regions
# MAGIC
# MAGIC INSERT INTO customer_transactions VALUES
# MAGIC ('TXN001', 1001, 2500.00, 'purchase', '2024-01-15 10:30:00', 'Amazon', 'Americas', 0.2),
# MAGIC ('TXN002', 1002, 5000.00, 'transfer', '2024-01-16 14:22:00', 'Bank Transfer', 'EMEA', 0.1),
# MAGIC ('TXN003', 1003, 15000.00, 'deposit', '2024-01-17 09:15:00', 'Salary Deposit', 'Asia Pacific', 0.1),
# MAGIC ('TXN004', 1004, 800.00, 'purchase', '2024-01-18 16:45:00', 'Grocery Store', 'EMEA', 0.3),
# MAGIC ('TXN005', 1005, 25000.00, 'transfer', '2024-01-19 11:20:00', 'Investment Transfer', 'Asia Pacific', 0.4),
# MAGIC ('TXN006', 1006, 1200.00, 'withdrawal', '2024-01-20 13:10:00', 'ATM Withdrawal', 'Americas', 0.2),
# MAGIC ('TXN007', 1007, 50000.00, 'deposit', '2024-01-21 08:30:00', 'Investment Return', 'EMEA', 0.1),
# MAGIC ('TXN008', 1008, 300.00, 'purchase', '2024-01-22 19:55:00', 'Gas Station', 'Americas', 0.6);
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### E3. Verify Customer Data
# MAGIC
# MAGIC Query both tables to verify the data was inserted correctly.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Query the customer_accounts table to see all customer records
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Query the customer_accounts table to see all customer records
# MAGIC SELECT * FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.customer_accounts') ORDER BY customer_id;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Query the customer_transactions table to see all transaction records
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Query the customer_transactions table to see all transaction records
# MAGIC SELECT * FROM customer_transactions ORDER BY transaction_date;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Create Business-Specific User-Defined Functions
# MAGIC
# MAGIC Develop UDFs that implement GlobalFinance Corp's specific business rules for data masking and filtering based on compliance requirements and user roles.

# COMMAND ----------

# MAGIC %md
# MAGIC ### F1. Create PII Masking Functions
# MAGIC
# MAGIC Create UDFs for masking different types of PII data according to financial services compliance requirements.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create a function to mask SSN for non-compliance users
# MAGIC ---- Only users in the 'Users' group should see full SSN, others see 'XXX-XX-XXXX'
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Create a function to mask SSN for non-compliance users
# MAGIC -- Only users in the 'Users' group should see full SSN, others see 'XXX-XX-XXXX'
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.mask_customer_ssn')(ssn STRING) RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('Users') THEN ssn
# MAGIC   ELSE 'XXX-XX-XXXX'
# MAGIC END;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create a function to mask email addresses for customer service users
# MAGIC ---- Only compliance users (Users group) should see full email, others see masked version
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Create a function to mask email addresses for customer service users
# MAGIC -- Only compliance users (Users group) should see full email, others see masked version
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.mask_customer_email')(email STRING) RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('Users') THEN email
# MAGIC   ELSE CONCAT(LEFT(email, 2), '***@', SUBSTRING_INDEX(email, '@', -1))
# MAGIC END;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### F2. Create Financial Data Protection Functions
# MAGIC
# MAGIC Create UDFs for protecting sensitive financial information based on user roles and business rules.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create a function to mask account balances for non-privileged users
# MAGIC ---- Only show balances above $10,000 to privileged users, others see 'CONFIDENTIAL'
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Create a function to mask account balances for non-privileged users
# MAGIC -- Only show balances above $10,000 to privileged users, others see 'CONFIDENTIAL'
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.mask_account_balance')(balance DECIMAL(15,2)) RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('Users') THEN CAST(balance AS STRING)
# MAGIC   WHEN balance <= 10000 THEN CAST(balance AS STRING)
# MAGIC   ELSE 'CONFIDENTIAL'
# MAGIC END;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create a function to filter high-risk customers for general analysts
# MAGIC ---- Only compliance officers should see high-risk customers, others see low and medium risk only
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Create a function to filter high-risk customers for general analysts
# MAGIC -- Only compliance officers should see high-risk customers, others see low and medium risk only
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.filter_by_risk_category')(risk_category STRING) RETURNS BOOLEAN
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('Users') THEN TRUE
# MAGIC   WHEN risk_category IN ('low', 'medium') THEN TRUE
# MAGIC   ELSE FALSE
# MAGIC END;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### F3. Test the Business Functions
# MAGIC
# MAGIC Validate that your UDFs work correctly with sample data before applying them to tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Test the mask_customer_ssn function with a sample SSN
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Test the mask_customer_ssn function with a sample SSN
# MAGIC
# MAGIC SELECT 
# MAGIC   '123-45-6789' AS original_ssn,
# MAGIC   IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.mask_customer_ssn')('123-45-6789') AS masked_ssn;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Test the mask_account_balance function with different balance amounts
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Test the mask_account_balance function with different balance amounts
# MAGIC
# MAGIC SELECT
# MAGIC   balance AS original_balance,
# MAGIC   IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.mask_account_balance')(balance) AS masked_balance
# MAGIC FROM VALUES
# MAGIC   (5000.00),
# MAGIC   (15000.00),
# MAGIC   (100000.00) AS t(balance);
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Test the filter_by_risk_category function with different risk levels
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Test the filter_by_risk_category function with different risk levels
# MAGIC
# MAGIC SELECT
# MAGIC   risk AS risk_category,
# MAGIC   IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.filter_by_risk_category')(risk) AS is_visible_to_analyst
# MAGIC FROM VALUES
# MAGIC   ('low'),
# MAGIC   ('medium'),
# MAGIC   ('high') AS t(risk);
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Implement Attribute-Based Access Control (ABAC) Policies
# MAGIC
# MAGIC Deploy centralized ABAC policies that automatically enforce GlobalFinance Corp's data governance rules based on the governed tags you applied earlier.

# COMMAND ----------

# MAGIC %md
# MAGIC ### G1. Create Column Masking ABAC Policies
# MAGIC
# MAGIC Implement ABAC policies that automatically mask PII columns based on their governed tags.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the base catalog and schema
# MAGIC USE CATALOG IDENTIFIER(CONFIG['abac_catalog_name']);
# MAGIC USE SCHEMA customer_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create an ABAC policy to mask SSN columns tagged with 'pii' = 'ssn'
# MAGIC ---- Use the mask_customer_ssn function you created earlier
# MAGIC ---- This policy automatically applies to any column in customer_data schema tagged with pii='ssn'
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Create an ABAC policy to mask SSN columns tagged with 'pii' = 'ssn'
# MAGIC -- Use the mask_customer_ssn function you created earlier
# MAGIC -- This policy automatically applies to any column in customer_data schema tagged with pii='ssn'
# MAGIC
# MAGIC CREATE OR REPLACE POLICY GlobalFinanceSSNMaskingPolicy
# MAGIC ON SCHEMA customer_data
# MAGIC COMMENT 'Masks customer SSN data for GDPR and CCPA compliance'
# MAGIC COLUMN MASK customer_data.mask_customer_ssn
# MAGIC TO `account users`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS
# MAGIC     hasTagValue('pii', 'ssn') AS ssn_column
# MAGIC ON COLUMN ssn_column;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create an ABAC policy to mask email columns tagged with 'pii' = 'email'
# MAGIC ---- Use the mask_customer_email function you created
# MAGIC ---- This policy automatically applies to any column in customer_data schema tagged with pii='email'
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Create an ABAC policy to mask email columns tagged with 'pii' = 'email'
# MAGIC -- Use the mask_customer_email function you created
# MAGIC -- This policy automatically applies to any column in customer_data schema tagged with pii='email'
# MAGIC
# MAGIC CREATE OR REPLACE POLICY GlobalFinanceEmailMaskingPolicy
# MAGIC ON SCHEMA customer_data
# MAGIC COMMENT 'Masks customer email addresses for privacy protection'
# MAGIC COLUMN MASK customer_data.mask_customer_email
# MAGIC TO `account users`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS
# MAGIC     hasTagValue('pii', 'email') AS email_column
# MAGIC ON COLUMN email_column;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### G2. Create Row Filtering ABAC Policy
# MAGIC
# MAGIC Implement an ABAC policy to filter rows based on risk categories and compliance requirements.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the base catalog and schema
# MAGIC USE CATALOG IDENTIFIER(CONFIG['abac_catalog_name']);
# MAGIC USE SCHEMA customer_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create a row filtering policy that uses the risk_category column
# MAGIC ---- This should filter out high-risk customers for non-compliance users
# MAGIC ---- Applies to any column in customer_data schema tagged with 'classification' = 'salary_masked'
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Create a row filtering policy that uses the risk_category column
# MAGIC -- This should filter out high-risk customers for non-compliance users
# MAGIC -- Applies to any column in customer_data schema tagged with 'classification' = 'salary_masked'
# MAGIC
# MAGIC CREATE OR REPLACE POLICY GlobalFinanceRiskFilterPolicy
# MAGIC ON SCHEMA customer_data
# MAGIC COMMENT 'Filters high-risk customers for regulatory compliance'
# MAGIC ROW FILTER customer_data.filter_by_risk_category
# MAGIC TO `account users`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS
# MAGIC     hasTagValue('classification', 'salary_masked') AS risk_col
# MAGIC USING COLUMNS (risk_col);
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Test and Validate ABAC Policy Enforcement
# MAGIC
# MAGIC Verify that your ABAC policies are working correctly by querying the protected data and observing the automatic masking and filtering behavior.

# COMMAND ----------

# MAGIC %md
# MAGIC ### H1. Test Column Masking Policies
# MAGIC
# MAGIC Query the customer accounts table to verify that PII and financial data masking is working.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Query the customer_accounts table to see the effect of ABAC column masking and row filtering policies
# MAGIC ---- Pay attention to:
# MAGIC ---- - SSN, email, phone columns (masked by PII policies)
# MAGIC ---- - account_balance, credit_score columns (masked by classification policy)
# MAGIC ---- - risk_category rows (high-risk customers filtered out for non-privileged users)
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Query the customer_accounts table to see the effect of ABAC column masking and row filtering policies
# MAGIC -- Pay attention to:
# MAGIC -- - SSN, email, phone columns (masked by PII policies)
# MAGIC -- - account_balance, credit_score columns (masked by classification policy)
# MAGIC -- - risk_category rows (high-risk customers filtered out for non-privileged users)
# MAGIC
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   email,
# MAGIC   ssn,
# MAGIC   phone,
# MAGIC   account_balance,
# MAGIC   credit_score,
# MAGIC   region,
# MAGIC   risk_category
# MAGIC FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.customer_accounts')
# MAGIC ORDER BY customer_id;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### H2. Test Row Filtering Policy
# MAGIC
# MAGIC Verify that high-risk customers are being filtered appropriately.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Query to specifically verify row filtering is working
# MAGIC ---- Count customers by risk category to confirm high-risk customers are filtered out
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Query to specifically verify row filtering is working
# MAGIC -- Count customers by risk category to confirm high-risk customers are filtered out
# MAGIC
# MAGIC SELECT 
# MAGIC   risk_category,
# MAGIC   COUNT(*) as customer_count
# MAGIC FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.customer_accounts')
# MAGIC GROUP BY risk_category
# MAGIC ORDER BY risk_category;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### H3. Test Policy Inheritance on New Tables
# MAGIC
# MAGIC Create a new table with similar tags to verify that ABAC policies automatically apply to new objects.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create a simple test table with a column tagged as 'pii' = 'ssn'
# MAGIC ---- This will verify that ABAC policies automatically apply to new tables
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Create a simple test table with a column tagged as 'pii' = 'ssn'
# MAGIC -- This will verify that ABAC policies automatically apply to new tables
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.test_customer_data') (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   ssn STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Tag the SSN column with the governed tag
# MAGIC ALTER TABLE IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.test_customer_data') ALTER COLUMN ssn SET TAGS ('pii' = 'ssn');
# MAGIC
# MAGIC -- Insert test data
# MAGIC INSERT INTO IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.test_customer_data') VALUES (1, 'Test Customer', '999-88-7777');
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Query the test table to verify ABAC policy inheritance
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Query the test table to verify ABAC policy inheritance
# MAGIC
# MAGIC SELECT * FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.test_customer_data');
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## I. Compare Legacy vs. ABAC Governance Approaches
# MAGIC
# MAGIC Understand the differences between legacy table-level controls and modern ABAC governance by implementing and comparing both approaches.

# COMMAND ----------

# MAGIC %md
# MAGIC ### I1. Temporarily Remove ABAC Policies
# MAGIC
# MAGIC Remove the ABAC policies to demonstrate legacy table-level controls.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the base catalog and schema
# MAGIC USE CATALOG IDENTIFIER(CONFIG['abac_catalog_name']);
# MAGIC USE SCHEMA customer_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Drop the ABAC policies we created to show the difference with legacy controls
# MAGIC ---- Drop all three policies: GlobalFinanceRiskFilterPolicy, GlobalFinanceEmailMaskingPolicy, and GlobalFinanceSSNMaskingPolicy
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Drop the ABAC policies we created to show the difference with legacy controls
# MAGIC -- Drop all three policies: GlobalFinanceRiskFilterPolicy, GlobalFinanceEmailMaskingPolicy, and GlobalFinanceSSNMaskingPolicy
# MAGIC
# MAGIC DROP POLICY GlobalFinanceRiskFilterPolicy ON SCHEMA customer_data;
# MAGIC DROP POLICY GlobalFinanceEmailMaskingPolicy ON SCHEMA customer_data;
# MAGIC DROP POLICY GlobalFinanceSSNMaskingPolicy ON SCHEMA customer_data;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### I2. Implement Legacy Table-Level Controls
# MAGIC
# MAGIC Apply traditional table-level column masks and row filters to demonstrate the legacy approach.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ensure we're in the correct context before applying legacy controls
# MAGIC USE CATALOG IDENTIFIER(CONFIG['abac_catalog_name']);
# MAGIC USE SCHEMA customer_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Apply a column mask directly to the SSN column using the legacy approach
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Apply a column mask directly to the SSN column using the legacy approach
# MAGIC
# MAGIC ALTER TABLE customer_accounts
# MAGIC ALTER COLUMN ssn
# MAGIC SET MASK customer_data.mask_customer_ssn;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Apply a row filter directly to the customer_accounts table using the legacy approach
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Apply a row filter directly to the customer_accounts table using the legacy approach
# MAGIC
# MAGIC ALTER TABLE customer_accounts
# MAGIC SET ROW FILTER customer_data.filter_by_risk_category ON (risk_category);
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### I3. Test Legacy Controls
# MAGIC
# MAGIC Query the table to see the legacy controls in action.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Query the customer_accounts table to see legacy table-level controls
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Query the customer_accounts table to see legacy table-level controls
# MAGIC
# MAGIC SELECT * FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.customer_accounts') ORDER BY customer_id;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### I4. Remove Legacy Controls and Restore ABAC
# MAGIC
# MAGIC Remove the legacy controls and restore the ABAC policies to show the preferred modern approach.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ensure we're in the correct context before applying legacy controls
# MAGIC USE CATALOG IDENTIFIER(CONFIG['abac_catalog_name']);
# MAGIC USE SCHEMA customer_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Remove the legacy table-level controls
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Remove the legacy table-level controls
# MAGIC
# MAGIC ALTER TABLE customer_data.customer_accounts ALTER COLUMN ssn DROP MASK;
# MAGIC ALTER TABLE customer_data.customer_accounts DROP ROW FILTER;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Restore the ABAC policies (recreate the key policies)
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Restore the ABAC policies (recreate the key policies)
# MAGIC
# MAGIC CREATE OR REPLACE POLICY GlobalFinanceSSNMaskingPolicy
# MAGIC ON SCHEMA customer_data
# MAGIC COMMENT 'Masks customer SSN data for GDPR and CCPA compliance'
# MAGIC COLUMN MASK customer_data.mask_customer_ssn
# MAGIC TO `account users`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS
# MAGIC     hasTagValue('pii', 'ssn') AS ssn_column
# MAGIC ON COLUMN ssn_column;
# MAGIC
# MAGIC CREATE OR REPLACE POLICY GlobalFinanceRiskFilterPolicy
# MAGIC ON SCHEMA customer_data
# MAGIC COMMENT 'Filters high-risk customers for regulatory compliance'
# MAGIC ROW FILTER customer_data.filter_by_risk_category
# MAGIC TO `account users`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS
# MAGIC     hasTagValue('classification', 'salary_masked') AS risk_col
# MAGIC USING COLUMNS (risk_col);
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## J. Business Impact Analysis and Reporting
# MAGIC
# MAGIC Analyze the business impact of your ABAC implementation and create reports that demonstrate compliance and governance effectiveness.

# COMMAND ----------

# MAGIC %md
# MAGIC ### J1. Create Compliance Summary Report
# MAGIC
# MAGIC Create a query that summarizes the governance coverage across your GlobalFinance Corp data.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create a compliance report showing:
# MAGIC ---- - Total customers by region
# MAGIC ---- - Risk category distribution  
# MAGIC ---- - Account types and average balances (where visible)
# MAGIC ---- This demonstrates that governance is working while still enabling business analytics
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Create a compliance report showing:
# MAGIC -- - Total customers by region
# MAGIC -- - Risk category distribution  
# MAGIC -- - Account types and average balances (where visible)
# MAGIC -- This demonstrates that governance is working while still enabling business analytics
# MAGIC
# MAGIC SELECT 
# MAGIC   'Customer Distribution by Region' as metric_type,
# MAGIC   region,
# MAGIC   COUNT(*) as count,
# MAGIC   NULL as avg_value
# MAGIC FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.customer_accounts') 
# MAGIC GROUP BY region
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Risk Category Distribution' as metric_type,
# MAGIC   risk_category as region,
# MAGIC   COUNT(*) as count,
# MAGIC   NULL as avg_value
# MAGIC FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.customer_accounts') 
# MAGIC GROUP BY risk_category
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Account Type Analysis' as metric_type,
# MAGIC   account_type as region,
# MAGIC   COUNT(*) as count,
# MAGIC   AVG(CAST(account_balance AS DOUBLE)) as avg_value
# MAGIC FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.customer_accounts')
# MAGIC GROUP BY account_type
# MAGIC
# MAGIC ORDER BY metric_type, region;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### J2. Validate Data Protection Effectiveness
# MAGIC
# MAGIC Create queries that demonstrate your data protection is working effectively.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Query to show that sensitive data is properly masked
# MAGIC ---- Display a few customer records focusing on the protected fields
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Query to show that sensitive data is properly masked
# MAGIC -- Display a few customer records focusing on the protected fields
# MAGIC
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   CONCAT(first_name, ' ', last_name) as customer_name,
# MAGIC   email,
# MAGIC   ssn,
# MAGIC   account_balance,
# MAGIC   region,
# MAGIC   risk_category
# MAGIC FROM IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.customer_accounts')
# MAGIC WHERE customer_id IN (1001, 1004, 1007)
# MAGIC ORDER BY customer_id;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## K. Clean Up Resources
# MAGIC
# MAGIC Clean up the lab environment by removing the created catalog and all associated objects.

# COMMAND ----------

# MAGIC %md
# MAGIC ### K1. Remove Test Objects
# MAGIC
# MAGIC Drop the test table created during the lab.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Drop the test_customer_data table
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Drop the test_customer_data table
# MAGIC
# MAGIC DROP TABLE IF EXISTS IDENTIFIER(CONFIG['abac_catalog_name'] || '.customer_data.test_customer_data');
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ### K2. Clean Up Complete Environment
# MAGIC
# MAGIC Drop the entire GlobalFinance Corp catalog and all its contents.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Drop the entire ABAC catalog created for this lab
# MAGIC
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary><strong>Click to reveal answer</strong></summary>
# MAGIC
# MAGIC <!-- Hidden source for the SQL answer -->
# MAGIC <textarea id="raw-answer-sql-display-config-all-catalogs" style="display:none;">
# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- Drop the entire ABAC catalog created for this lab
# MAGIC
# MAGIC DROP CATALOG IF EXISTS IDENTIFIER(CONFIG['abac_catalog_name']) CASCADE;
# MAGIC </textarea>
# MAGIC
# MAGIC <div class="code-block-dark" data-language="sql" data-source="raw-answer-sql-display-config-all-catalogs"></div>
# MAGIC </details>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet" id="prism-dark-theme" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         // Load SQL from hidden textarea and render inside <pre><code class="language-sql"> for Prism [web:4][web:14][web:194].
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#2d2d2d;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Conclusion
# MAGIC
# MAGIC **Congratulations!** You have successfully implemented a fine-grained access control framework for GlobalFinance Corp using Unity Catalog’s ABAC capabilities. Your solution protects sensitive customer financial data across regions while still enabling analytics, operations, and compliance teams to safely use that data.
# MAGIC
# MAGIC ### Key Governance Outcomes
# MAGIC
# MAGIC - **End-to-End Financial Data Protection:** You designed a governed catalog and schema for customer accounts and transactions, using tags to classify PII, financial sensitivity, and compliance scope.
# MAGIC - **Policy-Driven Masking and Filtering:** You implemented reusable masking and row-filtering UDFs and attached them to governed tags via ABAC policies, ensuring consistent enforcement across all tagged tables and columns.
# MAGIC - **Legacy vs. Modern Controls Comparison:** You compared table-level masks and row filters with centralized ABAC policies, demonstrating how tag-driven governance scales better and reduces configuration drift.
# MAGIC - **Compliance-Aware Analytics:** You validated that analysts and other consumers can still run meaningful business queries while sensitive fields are masked and high-risk segments are appropriately filtered.
# MAGIC
# MAGIC ### Professional Skills Developed
# MAGIC
# MAGIC - **Data Governance Design:** Structured catalogs, schemas, tags, and policies into a coherent financial data governance model aligned with real regulatory requirements.
# MAGIC - **Security-Focused SQL Engineering:** Used SQL to create UDFs, assign governed tags, define ABAC policies, and manage legacy row and column controls.
# MAGIC - **Regulatory Compliance Implementation:** Translated GDPR, CCPA, and banking obligations into concrete technical controls for PII and high-risk financial attributes.
# MAGIC - **Audit and Reporting Readiness:** Generated queries and summaries that demonstrate how governance controls operate, providing evidence suitable for internal stakeholders and external regulators.
# MAGIC
# MAGIC With these skills, you are now equipped to design and operationalize enterprise-grade data protection strategies that balance regulatory compliance, security, and business agility in any financial services environment.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>