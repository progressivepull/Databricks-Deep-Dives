# **14. Capstone Project**

**14.1 Project use case**

**📊 Project: Retail Sales Analysis for Store Optimization**

**🎯 Objective**

- Clean, prepare, and analyze sales + store data

- Generate insights to improve store performance and product trends

------------------------------------------------------------------------

**📁 Datasets**

1.  **Sales Data (CSV)**

    - Columns: sales_id, store_id, product_id, sale_date, quantity,
      total_amount

2.  **Store Data (CSV)**

    - Columns: store_id, store_region, store_size, open_date

------------------------------------------------------------------------

**🧹 Data Cleaning**

- Handle missing (null) values

- Remove duplicates and invalid rows

- Standardize date formats

------------------------------------------------------------------------

**🔄 Data Transformation**

- Join sales and store datasets (on store_id)

- Create new columns:

  - **Sales per square foot** = total_amount / store_size

  - **Sale year** (extracted from sale_date)

- Aggregate:

  - Total sales and quantity by **store** and **region**

------------------------------------------------------------------------

**📈 Analysis**

- Top 5 stores by total sales

- Top 5 products by total quantity sold

------------------------------------------------------------------------

**💾 Output**

- Save final results in **Parquet format**

**14.2 Solution**

**🚀 Project Execution Summary (Databricks + PySpark)**

**📥 1. Data Loading**

- Uploaded sales_data.csv and store_data.csv to **DBFS**

> <img src="./media/image1.png" style="width:4.12248in;height:5.58825in"
> alt="Graphical user interface, application AI-generated content may be incorrect." />
>
> <img src="./media/image2.png" style="width:4.83025in;height:5.80244in"
> alt="Graphical user interface, application AI-generated content may be incorrect." />
>
> <img src="./media/image3.png" style="width:6.24149in;height:3.07835in"
> alt="Graphical user interface, text, application, email AI-generated content may be incorrect." />

<img src="./media/image4.png" style="width:6.82784in;height:2.07972in"
alt="Graphical user interface, application, Teams AI-generated content may be incorrect." />

> <img src="./media/image5.png" style="width:5.3604in;height:3.93602in"
> alt="Graphical user interface, text, application, email AI-generated content may be incorrect." />

- Loaded into Spark DataFrames using:

spark.read.csv(..., header=True, inferSchema=True)

<img src="./media/image6.png" style="width:6.70516in;height:4.40778in"
alt="Graphical user interface, table AI-generated content may be incorrect." />

------------------------------------------------------------------------

**🧹 2. Data Cleaning**

**Sales Data (sales_df)**

- Filled nulls in:

  - quantity, total_amount → replaced with 0

<img src="./media/image7.png" style="width:7.15569in;height:1.35933in"
alt="Text AI-generated content may be incorrect." />

- Removed:

  - Duplicates (dropDuplicates)

<img src="./media/image8.png"
style="width:7.17712in;height:2.93056in" />

- Rows with nulls in sale_id, store_id, sale_date

<img src="./media/image9.png" style="width:6.95066in;height:2.80038in"
alt="Table AI-generated content may be incorrect." />

- Rows with negative quantity or total_amount

<img src="./media/image10.png"
style="width:6.78436in;height:3.76163in" />

**Store Data (store_df)**

- Removed rows with null store_id and open_date

- Replaced null store_size with **average store size**

<img src="./media/image11.png" style="width:5.72576in;height:5.64209in"
alt="Table AI-generated content may be incorrect." />

------------------------------------------------------------------------

**🔄 3. Data Transformation**

- Joined datasets on store_id (inner join) → combined_df

<img src="./media/image12.png" style="width:6.84219in;height:1.3243in"
alt="A picture containing text AI-generated content may be incorrect." /><img src="./media/image13.png" style="width:1.04438in;height:0.58865in"
alt="Shape AI-generated content may be incorrect." />

- Added new columns:

  - **Year** from sale_date

  - **Sales per sqft** = total_amount / store_size (rounded)

<img src="./media/image14.png" style="width:6.95663in;height:2.78265in"
alt="Table AI-generated content may be incorrect." />

------------------------------------------------------------------------

**📊 4. Analysis**

- **Sales by Store & Region**

  - Aggregated total sales and quantity using Spark SQL

> <img src="./media/image15.png" style="width:5.95532in;height:2.37709in"
> alt="Graphical user interface, text, application AI-generated content may be incorrect." />

- **Top 5 Products**

  - Based on total quantity sold (group + order + limit)

> <img src="./media/image16.png" style="width:6.43964in;height:3.32393in"
> alt="Graphical user interface, text, application, email AI-generated content may be incorrect." />

- **Top 5 Stores**

  - Based on total sales (sorted + limited)

<img src="./media/image17.png" style="width:6.69959in;height:2.51534in"
alt="Graphical user interface, text, application, email AI-generated content may be incorrect." />

------------------------------------------------------------------------

**💾 5. Output**

- Saved results as **Parquet files** in DBFS:

  - top_products

  - top_store

<img src="./media/image18.png" style="width:6.40167in;height:1.90789in"
alt="Graphical user interface, text, application, email AI-generated content may be incorrect." />

------------------------------------------------------------------------

**✅ End Result**

- Cleaned datasets

- Enriched data with new metrics

- Generated business insights (top stores/products)

- Stored outputs for downstream use

# [Content](./../content.md)