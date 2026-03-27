# **Visualization**

**🔹 Creating Dashboards in Databricks**

- Each Bar Chart, Pie Graph, etc... is called Visualization.

<img src="./media/image1.png" style="width:6.5in;height:4.4875in" />

<img src="./media/image2.png" style="width:6.5in;height:1.70208in" />

- Start with a **blank dashboard canvas**.

> <img src="./media/image3.png" style="width:6.5in;height:4.92292in"
> alt="Graphical user interface AI-generated content may be incorrect." />

- Add:

  - Data (via SQL query or selecting a dataset)

  - Visualizations (Dashboard)

  - Filters

  - Text boxes (for titles/headers)

- Data can come from:

> <img src="./media/image4.png"
> style="width:6.21337in;height:5.96846in" />
>
> <img src="./media/image5.png"
> style="width:6.21065in;height:7.77752in" />
>
> <img src="./media/image6.png" style="width:6.5in;height:6.27361in" />
>
> <img src="./media/image7.png" style="width:6.5in;height:3.07292in" />
>
> From the Notebook where you can find in the Workspace:
>
> <img src="./media/image8.png" style="width:6.5in;height:3.65556in"
> alt="Graphical user interface, text AI-generated content may be incorrect." />

- Custom SQL queries

- Existing datasets from the catalog

- Notebook queries (added to dashboard)

------------------------------------------------------------------------

**🔹 Sample Dataset Used**

- Dataset: **Bake House – Sales Transactions**

- Key columns:

  - Transaction ID, Customer ID, Franchise ID

  - Date & Time

  - Product

  - Quantity

  - Unit Price

  - Total Price

  - Payment Method

------------------------------------------------------------------------

**🔹 Visualization 1: Bar Chart (Total Price by Product)**

- SQL query:

> **SELECT** product, SUM(total_price)
>
> **FROM** sample.bakehouse.sales_transaction
>
> **GROUP BY** product
>
> **ORDER BY** SUM(total_price) **DESC**

<img src="./media/image9.png" style="width:6.49443in;height:9.62098in"
alt="Graphical user interface, application AI-generated content may be incorrect." />

- Built a **horizontal bar chart**

<img src="./media/image10.png" style="width:6.5in;height:3.65556in" />

- X-axis → Sum of total price

- Y-axis → Product

<!-- -->

- **Improvements made**:

> <img src="./media/image11.png" style="width:6.5in;height:3.65556in" />

- Added data labels

<img src="./media/image12.png" style="width:6.5in;height:2.94097in" />

- Sorted descending (highest to lowest sales)

<img src="./media/image13.png" style="width:6.5in;height:1.84653in" />

- Added title and description

<img src="./media/image14.png" style="width:6.5in;height:3.11181in" />

- Applied gradient color scale

<img src="./media/image15.png" style="width:6.5in;height:2.99653in" />

- Customized gradient scale

------------------------------------------------------------------------

**🔹 Visualization 2: Pie Chart (Payment Method Breakdown)**

**Notice the Data is sales_transactions**

<img src="./media/image16.png" style="width:6.5in;height:3.65556in" />

- Used full dataset (no custom SQL required).

- Chart type: Pie chart

  - Angle → Count of payment method

  - Color → Payment method

- Avoided count distinct (only 3 methods).

- Added:

  - Labels

  - Custom display name

  - Improved title formatting

------------------------------------------------------------------------

**🔹 Visualization 3: Line Chart (Quantity Sold Over Time)**

<img src="./media/image17.png"
style="width:4.69656in;height:3.35469in" />

<img src="./media/image18.png" style="width:6.5in;height:5.05625in"
alt="A picture containing graphical user interface AI-generated content may be incorrect." />

- Chart type: Line chart

> <img src="./media/image19.png" style="width:2.96225in;height:6.779in" />

- X-axis → Date (grouped daily)

- Y-axis → Sum of quantity

<!-- -->

- Automatically aggregates numeric data.

- Renamed axes for clarity:

<img src="./media/image20.png" style="width:6.5in;height:5.74375in"
alt="A screenshot of a computer AI-generated content may be incorrect." />

- Date

- Quantity Sold

<!-- -->

- Added chart title.

------------------------------------------------------------------------

**🔹 Customizing Dashboards**

- Add:

<img src="./media/image21.png" style="width:6.5in;height:4.63264in" />

- Titles

- Descriptions

**🔹 Adding Filters (Global Filters)**

<img src="./media/image22.png"
style="width:5.79196in;height:3.46546in" />

- **Adding the filter**

<img src="./media/image23.png" style="width:4.48135in;height:4.14588in"
alt="Graphical user interface, application AI-generated content may be incorrect." />

- Filters are important for users (customers, managers, stakeholders).

- Example:

  - **Range slider filter on Quantity**

  - Allows filtering transactions by quantity range.

<img src="./media/image24.png" style="width:6.5in;height:2.61111in"
alt="A screenshot of a computer AI-generated content may be incorrect." />

- Filter types include:

  - Single value

  - Multiple values

  - Date picker

  - Date range

  - Text entry

  - Range slider

------------------------------------------------------------------------

**🔹 Important Lesson on Filters & Data Sets**

<img src="./media/image25.png" style="width:6.5in;height:3.65556in" />

- Filters only apply to visualizations using the **same dataset**.

<img src="./media/image26.png" style="width:6.5in;height:3.58542in"
alt="Graphical user interface, text, application AI-generated content may be incorrect." />

- If a visualization is built from a different query/dataset:

  - The filter will NOT affect it.

- Solution:

  - Rebuild visualization using the dataset connected to the filter.

------------------------------------------------------------------------

**🔹 Key Takeaways**

- You can build dashboards from:

  - SQL queries

  - Notebooks

  - Direct dataset selection

- Always think about:

  - End goal

  - Required aggregations

  - Data transformations

  - User filtering needs

- Clean formatting and thoughtful naming improve dashboard usability.

- Understanding dataset relationships is crucial for proper filtering.

# [Context](./../context.md)