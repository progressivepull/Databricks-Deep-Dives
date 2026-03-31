# 2. Getting Started with Databricks

## 2.1 Understanding the Databricks File System (DBFS)

**Understanding Databricks File System (DBFS)**

- **DBFS (Databricks File System)** is the core storage layer of
  Databricks.

- Used to **read and write data** within Databricks.

- Acts like a **local file system** for users inside Databricks.

- Built on top of **cloud storage infrastructure**.

------------------------------------------------------------------------

**What is DBFS?**

- A **distributed file system**.

- Abstracts underlying cloud storage.

- Provides a **unified interface** to manage data.

- Data is physically stored in:

  - Azure Data Lake Storage (ADLS)

  - AWS S3

  - Google Cloud Storage
    (based on chosen cloud provider)

------------------------------------------------------------------------

**Key Features of DBFS**

**1. Unified Interface**

- Access files and directories like a local system.

- Supports basic file commands:

  - ls (list)

  - cp (copy)

  - mv (move)

**2. Seamless Integration**

- Fully integrated with:

  - Notebooks

  - Clusters

  - Workflows

**3. Cloud-Based Object Storage**

- Files saved in DBFS are stored in cloud storage.

- Highly scalable and distributed.

**4. Scalability**

- Can store:

  - Small files (MB/GB)

  - Large-scale data (TB and beyond)

**5. Data Persistence**

- Data remains stored permanently.

- Not affected by cluster start/stop.

- Works like a persistent disk.

**6. Multiple Storage Layers**

- Temporary storage for small files.

- Supports mounting external storage:

  - ADLS

  - AWS S3

- Mounted storage can be accessed seamlessly.

------------------------------------------------------------------------

**Directory Structure**

- Uses **Unix-like directory structure**.

**Absolute Path**

- Starts with /

- Specifies full path from root.

- Example: /mnt/data/file.csv

**Relative Path**

- Path relative to current directory.

- Example: D2/file.csv

**Programmatic Access**

- Use dbfs:/ prefix.

- Example:

  - dbfs:/mnt/data/sample.csv

------------------------------------------------------------------------

**Use Cases of DBFS**

- **Data Loading**

  - Read files into DataFrames.

- **Data Writing**

  - Save processed/cleaned data.

- **Backup and Archiving**

  - Store backup files.

- **File Sharing**

  - Share files via mounted locations.

------------------------------------------------------------------------

**Limitations of DBFS**

- Dependent on underlying cloud storage.

- Files cannot be directly accessed outside Databricks workspace.

  - No direct local machine access.

------------------------------------------------------------------------

**Overall Importance**

- DBFS is a fundamental component of Databricks.

- Enables scalable, persistent, and unified data storage.

- Central to data processing workflows in Databricks.

**NOTES**

To read or interact with a file located
at dbfs:/mnt/data/sample.csv in [Databricks](https://www.google.com/url?sa=i&source=web&rct=j&url=https://www.databricks.com/&ved=2ahUKEwi_ya6rpYmTAxUZrYkEHZcxOysQy_kOegYIAQgCEAE&opi=89978449&cd&psig=AOvVaw1u8x6r4-4JHdT615amfQaL&ust=1772818404744000),
you can use Spark, Pandas, or Databricks Utilities.

**1. Using PySpark (Recommended)**

This is the most common way to read data into a [Spark
DataFrame](https://www.google.com/url?sa=i&source=web&rct=j&url=https://docs.databricks.com/aws/en/query/formats/csv&ved=2ahUKEwi_ya6rpYmTAxUZrYkEHZcxOysQy_kOegYIAQgEEAE&opi=89978449&cd&psig=AOvVaw1u8x6r4-4JHdT615amfQaL&ust=1772818404744000) for
large-scale processing. 

**Python**   
``` python

# Read the CSV file into a Spark DataFrame*

df = spark.read.format("csv")

.option("header", "true")

.option("inferSchema", "true")

.load("dbfs:/mnt/data/sample.csv")

# Display the first 10 rows*

display(df)

```

**2. Using Pandas**

Standard Pandas (pd.read_csv) cannot directly access
the dbfs:/ protocol. You must use the local file API path by
replacing dbfs: with /dbfs/. 

**Python** 
``` python

import pandas as pd

# Use the local file API mount point to read with Pandas*

file_path = "/dbfs/mnt/data/sample.csv"

pdf = pd.read_csv(file_path)

# Show the first few rows*

print(pdf.head())

```

**3. Using Databricks Utilities (dbutils)**

Use dbutils.fs to manage the file, such as checking if it exists or
listing directory contents. 

``` python

# List files in the directory to verify location*

display(dbutils.fs.ls("/mnt/data/"))

# Read the first few lines of the file as a string*

head_content = dbutils.fs.head("dbfs:/mnt/data/sample.csv", 1024)

print(head_content)

```

**4. Using SQL**

If you prefer SQL, you can query the file directly if the workspace is
configured to read files from DBFS. 

``` sql

SELECT \* FROM csv.\`dbfs:/mnt/data/sample.csv\`

```

**Common Tips:**

- **Path Formats**: Use dbfs:/ for Spark/dbutils and /dbfs/ for local
  Python libraries like Pandas or OS commands.

- **Performance**: For large files, use Spark
  with inferSchema or [define a custom
  schema](https://www.google.com/url?sa=i&source=web&rct=j&url=https://medium.com/@priyanka.gilbert24/step-by-step-guide-reading-csv-files-creating-a-custom-schema-and-storing-data-in-databricks-58fbd0963839&ved=2ahUKEwi_ya6rpYmTAxUZrYkEHZcxOysQy_kOegYIAQgREAI&opi=89978449&cd&psig=AOvVaw1u8x6r4-4JHdT615amfQaL&ust=1772818404744000) using StructType to
  speed up the loading process. 

Would you like to see how to **write** data back to that same directory
or **convert** this CSV into a Delta table?

In [Databricks](https://www.google.com/url?sa=i&source=web&rct=j&url=https://www.databricks.com/&ved=2ahUKEwianKnTpomTAxV8PkQIHb-yBkcQy_kOegYIAQgCEAE&opi=89978449&cd&psig=AOvVaw3pRd1iY5rk_DF9AvBRE5fO&ust=1772818757000000),
the concept of a "relation path" typically refers to the **relative
path** used to reference files or other notebooks relative to your
current working directory. The behavior of these paths varies depending
on the Databricks Runtime (DBR) version and the file system being
accessed. 

**1. Relative Paths for Notebooks and Modules**

You can use relative paths to run other notebooks or import Python/R
modules stored alongside your current notebook. 

- **Running Notebooks**: Use %run with ./ for the same directory
  or ../ to move up a level.

  - Example: %run ./helper_notebook

- **Python Imports**: In **Databricks Runtime 14.0 and above**, the
  default current working directory (CWD) is the directory containing
  the notebook. This allows for standard relative imports.

  - Example: from . import my_module 

**2. Relative Paths for Data Files**

How you access data files depends on whether you are using the Spark API
or local Python libraries (like Pandas). 

| **Access Method ** | **Path Convention** | **Description** |
|----|----|----|
| **Spark API** | dbfs:/path/to/file | Requires a fully qualified path or the dbfs:/ scheme for most operations. |
| **Local File API** | /dbfs/path/to/file | Used by standard Python libraries (Pandas, OS). It treats DBFS as a local mount point. |
| **Workspace Files** | file:/Workspace/Users/... | Required when using Spark to read small data files stored directly in the workspace. |
| **Unity Catalog** | /Volumes/catalog/schema/... | Uses POSIX-style paths for Unity Catalog Volumes. |

**3. Key Path Formats by Location**

- **DBFS (Root/Mounts)**: Typically accessed
  via dbfs:/mnt/my_data/ (Spark) or /dbfs/mnt/my_data/ (Local API).

- **Unity Catalog Volumes**: Standardized
  as /Volumes/\<catalog\>/\<schema\>/\<volume_name\>/\<path_to_file\>.

- **Ephemeral Storage**: Local to the driver node, accessed via /tmp/ or
  by prefixing with file:/ for Databricks Utilities. 

**Summary of Special Symbols**

- ./ : Current directory.

- ../ : Parent directory.

- / : The root of the respective file system (e.g., driver root for
  local API). 

Are you trying to set up **relative imports** for a Python project, or
are you looking for the best way to reference **data files** in a
production pipeline?

## 2.2 Load sample data DBFS

- After understanding **DBFS (Databricks File System)**, the next step
  is to **upload sample data** for analysis, cleaning, and processing.

- Sample files (CSV, JSON, Parquet) are provided in the course exercise
  folder.

**Two ways to upload data into DBFS:**

1.  **From Databricks Workspace UI:**

    - Click **+ New → Add or Upload Data → Upload files to DBFS**

> <img src="./media/image1.png" style="width:2.49386in;height:1.11401in"
> alt="Graphical user interface, text, application AI-generated content may be incorrect." />
>
> <img src="./media/image2.png" style="width:3.56356in;height:3.60154in"
> alt="Graphical user interface, application AI-generated content may be incorrect." />

- Select or drag files (e.g., CSV)

> <img src="./media/image3.png" style="width:4.72188in;height:1.88622in"
> alt="Graphical user interface, text, application AI-generated content may be incorrect." />

- Files are stored in: /FileStore/tables/customer.csv

> <img src="./media/image4.png" style="width:4.6839in;height:2.90528in"
> alt="Graphical user interface, text, application, email AI-generated content may be incorrect." />

2.  **From a Notebook:**

    - Use the **Upload Data to DBFS** option inside the notebook

> <img src="./media/image5.png" style="width:2.91161in;height:3.75345in"
> alt="Graphical user interface, application AI-generated content may be incorrect." />

- Default path: /FileStore/shared_uploads/\<your-email\>/

> <img src="./media/image6.png" style="width:4.05727in;height:2.87364in"
> alt="Graphical user interface, text, application, email AI-generated content may be incorrect." />
>
> <img src="./media/image7.png" style="width:4.08259in;height:2.72173in"
> alt="Graphical user interface, text, application, chat or text message AI-generated content may be incorrect." />

- You can change the destination path (e.g., /FileStore/tables/)

<!-- -->

- Multiple file formats (CSV, JSON, Parquet) can be uploaded.

- File location depends on the upload method, but you can customize it.

**Key idea:**
DBFS acts as centralized storage, and you can upload data either via the
UI or notebook, choosing where it’s stored.

## 2.3 Browse and explore in DBFS

<img src="./media/image8.png" style="width:1.15832in;height:1.80393in"
alt="Graphical user interface, text, application AI-generated content may be incorrect." />

**1. Using the UI (Catalog tab):**

- Go to the **Catalog tab** and enable the **DBFS file browser** if it’s
  not visible (via Settings → Advanced → enable DBFS browser).

<img src="./media/image9.png"
style="width:1.35453in;height:2.56349in" />

<img src="./media/image10.png"
style="width:6.02577in;height:2.58247in" />

- Refresh the page.

- Click **“Browse DBFS”** to view files.

> <img src="./media/image11.png" style="width:6.4815in;height:2.05079in"
> alt="Graphical user interface, text, application, email AI-generated content may be incorrect." />

- Navigate to **FileStore → tables** to see uploaded files (e.g.,
  customer.csv, product.json, transaction.parquet).

> <img src="./media/image12.png" style="width:3.75989in;height:2.16556in"
> alt="Graphical user interface, application AI-generated content may be incorrect." />
>
> <img src="./media/image13.png" style="width:3.67749in;height:2.0318in"
> alt="Graphical user interface, text, application AI-generated content may be incorrect." />

- You can also upload files directly from this interface.

**2. Using a notebook command:**

- Use the magic command %fs ls /FileStore/tables

- This lists files along with details like size and modification time.

<img src="./media/image14.png" style="width:4.05094in;height:2.4812in"
alt="Table AI-generated content may be incorrect." />

# [Content](./../content.md)
