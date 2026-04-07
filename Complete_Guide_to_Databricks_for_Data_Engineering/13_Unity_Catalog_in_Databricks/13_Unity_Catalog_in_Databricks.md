**13. Unity Catalog in Databricks**

13.1 Understand the Unity Catalog

**Unity Catalog**

**Unity Catalog** is a **centralized data governance solution** in
Databricks used to **manage, secure, and monitor data** across multiple
workspaces and teams.

------------------------------------------------------------------------

**Key Features:**

- **Centralized Governance**

  - Manage permissions (read/write) across the organization from one
    place

- **Fine-Grained Access Control**

  - Control access at **table, column, and role level**

- **Data Lineage**

  - Track how data flows and transforms across pipelines

- **Cross-Workspace & Cross-Cloud Sharing**

  - Share data across teams, workspaces, and cloud platforms (AWS,
    Azure, GCP)

- **Unified Metadata Management**

  - Central repository for all data objects

- **Audit Logging**

  - Track user activity and system operations for compliance

- **Multi-Format Support**

  - Works with CSV, JSON, Parquet, etc.

------------------------------------------------------------------------

**Hierarchy Structure:**

- **Metastore** (top-level, centralized metadata layer)

  - **Catalog**

    - **Schema**

      - **Tables / Views**

------------------------------------------------------------------------

**Benefits:**

- **Enterprise-wide data governance**

- **Improved collaboration across teams**

- **Better data visibility and traceability (lineage)**

- **Supports regulatory compliance and auditing**

------------------------------------------------------------------------

**Limitations:**

- **Complex initial setup**

- **Region-specific metastore**

  - May require multiple metastores for different regions

------------------------------------------------------------------------

**Bottom Line:**

Unity Catalog provides a **centralized, secure, and scalable way to
govern data across an organization**, making it essential for
**enterprise-level data management, collaboration, and compliance** in
Databricks.

# [Content](./../content.md)
