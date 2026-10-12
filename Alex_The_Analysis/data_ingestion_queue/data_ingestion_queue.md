# **Data Ingestion Queue**

- **Starting Point**

  - Fresh Databricks workspace with no uploaded or ingested data.

  - Working inside **Catalog → Workspace → Default schema**.

<img src="./media/image1.png" style="width:6.83435in;height:6.06746in"
alt="A screenshot of a computer AI-generated content may be incorrect." />

------------------------------------------------------------------------

**Uploading Files to a Volume**

<img src="./media/image2.png"
style="width:7.06349in;height:2.17988in" />

<img src="./media/image3.png" style="width:7.06319in;height:4.62209in"
alt="A screenshot of a computer AI-generated content may be incorrect." />

<img src="./media/image4.png"
style="width:7.04037in;height:6.10317in" />

- Created a new **Volume** called ***YouTube series***.

<img src="./media/image5.png" style="width:7.06811in;height:1.78968in"
alt="A screenshot of a computer AI-generated content may be incorrect." />

- Uploaded a **customers.csv** file into the **volume**.

<img src="./media/image6.png"
style="width:7.16806in;height:3.99555in" />

<img src="./media/image7.png" style="width:7.16856in;height:4.2619in"
alt="Graphical user interface, text AI-generated content may be incorrect." />

- Files stored in volumes remain as raw files (not tables).

- CSV file structure:

<img src="./media/image8.png" style="width:7.37695in;height:4.5754in" />

- Columns: **customer_id**, **first_name**, **last_name**, **country**,
  **date**

<img src="./media/image9.png"
style="width:7.30058in;height:3.22619in" />

- Comma-separated values.

**Querying a CSV File Directly (Without Creating a Table)**

<img src="./media/image10.png"
style="width:7.31491in;height:3.12698in" />

- Copied file path from the volume.

<img src="./media/image11.png" style="width:7.33105in;height:3.88115in"
alt="A screenshot of a computer AI-generated content may be incorrect." />

- Used SQL query:

<img src="./media/image12.png" style="width:7.11062in;height:6.37698in"
alt="Text AI-generated content may be incorrect." />

SELECT \* FROM csv.\`\<file_path\>\`

- Databricks reads CSV as a table format on-the-fly.

- No need to create a formal table to query CSV data.

------------------------------------------------------------------------

**Creating a Table from JSON**

<img src="./media/image13.png"
style="width:7.21489in;height:2.27778in" />

- Used **Create → Table** option.

<img src="./media/image14.png" style="width:7.20365in;height:3.86508in"
alt="Graphical user interface, text AI-generated content may be incorrect." />

- Uploaded **orders.json**.

<img src="./media/image15.png"
style="width:7.12248in;height:2.93878in" />

- Databricks:

  - Automatically converts JSON into tabular format.

  - Extracts columns (customer_id, order_date, order_id, product_id,
    quantity, total_amount).

<img src="./media/image16.png"
style="width:7.18086in;height:2.90873in" />

- Created table named **orders**.

- Tables and Volumes are separate:

<img src="./media/image17.png"
style="width:7.16711in;height:5.36002in" />

- **Volumes** → store raw files.

- **Tables** → structured, query-ready datasets.

------------------------------------------------------------------------

**Querying Created Table**

- Ran:

<img src="./media/image18.png"
style="width:6.88601in;height:2.11656in" />

<img src="./media/image19.png"
style="width:6.82464in;height:3.14254in" />

SELECT \* FROM orders;

<img src="./media/image20.png" style="width:6.5in;height:4.12431in" />

- Table resides under default schema.

<img src="./media/image21.png" style="width:6.5in;height:1.87708in" />

- Fully qualified naming (workspace.default.orders) optional if already
  in schema.

------------------------------------------------------------------------

**Data Ingestion from External Sources**

<img src="./media/image22.png"
style="width:7.09159in;height:3.9875in" />

- Databricks supports connectors for:

  - Salesforce

  - Workday

  - ServiceNow

  - Google Analytics

  - Azure SQL Server

  - Amazon S3

Fivetran is a cloud-based data integration platform that automates the
Extract‑Load‑Transform (ELT) process, **moving data from many sources
into a data warehouse with minimal engineering effort**. It focuses on
fully managed, non-code pipelines that keep data continuously synced.

- Demonstrated ingestion via **Google Drive using Fivetran**:

<img src="./media/image23.png"
style="width:6.22448in;height:7.02648in" />

- Connected Google Drive folder.

- Enabled sharing access.

<img src="./media/image24.png" style="width:6.5in;height:2.91111in" />

- Synced data.

<img src="./media/image25.png" style="width:6.5in;height:3.66806in" />

# Google

<img src="./media/image26.png"
style="width:7.11777in;height:3.13456in" />

<img src="./media/image27.png" style="width:6.5in;height:2.72778in" />

<img src="./media/image28.png"
style="width:6.91139in;height:5.22268in" />

# Fivetran

<img src="./media/image29.png"
style="width:7.09128in;height:4.85783in" />

<img src="./media/image30.png" style="width:6.5in;height:3.6375in" />\
<img src="./media/image31.png" style="width:6.5in;height:3.66806in" />

<img src="./media/image32.png" style="width:6.5in;height:3.66806in" />

<img src="./media/image33.png" style="width:6.5in;height:3.66806in" />

<img src="./media/image34.png" style="width:6.5in;height:2.48056in" />

<img src="./media/image35.png" style="width:6.5in;height:2.58264in" />

<img src="./media/image36.png" style="width:6.5in;height:2.27014in" />

Orders CSV appeared as a table under a new **Google Drive schema**.

<img src="./media/image37.png" style="width:6.5in;height:3.37153in" />

- Added system column: \_fivetran_synced (tracks sync time).

------------------------------------------------------------------------

**Working with Data: SQL Editor vs Notebook**

<img src="./media/image38.png" style="width:6.5in;height:6.93194in" />

<img src="./media/image39.png" style="width:6.5in;height:2.20625in" />

**✨ What Markdown Is**

- Markdown is a simple way to add formatting—like headings, bold text,
  lists, links, and more—using plain text characters. It’s popular
  because:

  - It’s easy to read even before it’s rendered

  - It works almost everywhere (GitHub, Reddit, documentation, notes
    apps)

  - It keeps your writing clean and distraction‑free

**Think of it as a shorthand for formatting.**

[**Markdown Guide**](https://www.markdownguide.org/)

**SQL Editor**

- Best for:

  - Simple queries

  - Joins

  - Aggregations

  - Quick exploration

- Queries can be saved and shared.

**Notebooks**

<img src="./media/image40.png" style="width:6.5in;height:4.86111in" />

- Support:

  - SQL

  - Python

  - Scala

  - R

  - Markdown

- Ideal for:

  - Data transformations

  - Complex logic

  - Documentation

  - Team collaboration

- **Multiple languages** can be used in the **same notebook**.

------------------------------------------------------------------------

**Querying in Notebook Examples**

<img src="./media/image41.png" style="width:6.5in;height:4.09097in" />

**SQL in Notebook**

SELECT \* FROM orders;

**Python in Notebook**

<img src="./media/image42.png" style="width:6.5in;height:4.14375in" />

df = spark.table("orders")\
display(df)

- Data read as a Spark DataFrame.

- Output shown in tabular format.

------------------------------------------------------------------------

**Key Takeaways**

- Data can be:

  - Queried directly from raw files (CSV).

  - Converted into structured tables (JSON → table).

  - Synced from external systems (Google Drive via Fivetran).

- Volumes ≠ Tables.

- SQL Editor is great for querying.

- Notebooks are better for full workflows and collaboration.

- Most Databricks work revolves around querying and transforming data.

Next step preview:

- Analyzing data with SQL.

- Building visualizations in Databricks.

# [Content](./../content.md)