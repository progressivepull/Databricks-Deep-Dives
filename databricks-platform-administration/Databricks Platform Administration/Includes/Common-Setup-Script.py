# Databricks notebook source
# ----------------------------------------------------------------------------------------
# Build a Metadata Table and Share a Unified `CONFIG` Map in SQL and Dictionary in Python
# ----------------------------------------------------------------------------------------

# COMMAND ----------

# ---------------------------------------------------------
# Get the 'username' of the current user
# ---------------------------------------------------------
def get_current_account_username() -> str:
    return spark.sql("SELECT current_user() AS user").collect()[0][0]

# COMMAND ----------

# ------------------------------------------------------------------
# Convert a string to a safe name string supported in Unity Catalog
# ------------------------------------------------------------------

def uc_safename(name: str):
    """
    UC-safe name based on:
    [https://docs.databricks.com/en/sql/language-manual/sql-ref-names.html](https://docs.databricks.com/en/sql/language-manual/sql-ref-names.html)

    Rules applied:
    - replace '.', ' ', '/' with '_'
    - drop ASCII control chars (0x00–0x1F) and DEL (0x7F)
    - convert to lowercase
    - truncate to 255 characters
    - drop domain part after '@' (keep only part before '@')
    """

    return ''.join(
            map(
                lambda x: '_' if x in ['.',' ','/'] else '' if ord(x) < 0x20 or ord(x) == 0x7f else x,
                name
            )
        ).lower()[0:255].split('@')[0]

# COMMAND ----------

# -------------------------------------------
# Catalog name for "base" user catalog
# -------------------------------------------
def get_catalog_name() -> str:
    current_user = get_current_account_username()
    return f"db_{uc_safename(current_user)}"

# COMMAND ----------

# -------------------------------------------
# Catalog name for "base" user catalog
# -------------------------------------------
def gen_catalog_name(name: str = "sample") -> str:
    if name == "":
        name = "sample"
    else:
        name = name.lower()
    current_user = get_current_account_username()
    return f"db_{uc_safename(name)}_{uc_safename(current_user)}"

# COMMAND ----------

# ---------------------------------------------------------
# Build CONFIG dict and SQL MAP variable from user context
# ---------------------------------------------------------

# OVERVIEW:
# Builds CONFIG dictionary with base catalog and optional component catalogs.

# generate_components = True:
# → Includes all component catalogs (system, office, abac, lakehouse) in CONFIG

# generate_components = False:
# → Includes only base catalog (no component catalogs)

# components = ["system", "office", "abac", "lakehouse"]:
# → Defines which component catalogs to create; each generates a key like
#   "<name>_catalog_name" in CONFIG

# CUSTOMIZE:
#   • Set generate_components = False to skip component catalogs
#   • Edit components list to add/remove catalog types

# ---------------------------------------------------------

# Toggle: build component catalogs or not
generate_components = True  # set to False to only build base catalog

# Base user catalog (key will be 'catalog_name')
catalog_name = get_catalog_name()

# Component names – only defined once (can be overridden externally)
components = ["system", "office", "abac", "lakehouse"]

# Conditionally build component catalog names with safety around components
try:
    if generate_components and isinstance(components, (list, tuple)) and len(components) > 0:
        component_catalogs = {
            f"{comp}_catalog_name": gen_catalog_name(comp)
            for comp in components
            if isinstance(comp, str) and comp.strip() != ""
        }
    else:
        # Treat as generate_components = False if list is empty/invalid
        component_catalogs = {}
except Exception:
    # On any error while building components, fall back to base-only config
    component_catalogs = {}

# Get current user and timestamp
current_user = get_current_account_username()
created_ts = spark.sql("SELECT current_timestamp() AS ts").collect()[0][0]

# Build CONFIG dict (base + optional components)
CONFIG = {
    "catalog_name": catalog_name,
    "username": current_user,
    "created_timestamp": str(created_ts),
}
CONFIG.update(component_catalogs)

# Convert dict into a SQL map(...) literal safely
pairs = []
for k, v in CONFIG.items():
    safe_k = k.replace(".", "_")
    safe_v = str(v).replace("'", "''")  # escape single quotes
    pairs.append(f"'{safe_k}'")
    pairs.append(f"'{safe_v}'")

map_literal = ", ".join(pairs)

# Create SQL MAP variable CONFIG
spark.sql(f"""
    DECLARE OR REPLACE VARIABLE CONFIG MAP<STRING,STRING>
    DEFAULT map({map_literal})
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- -----------------------------------------------------------