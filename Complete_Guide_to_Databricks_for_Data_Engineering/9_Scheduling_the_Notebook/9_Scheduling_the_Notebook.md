**9. Scheduling the Notebook**

9.1 Notebook job scheduling

**Notebook Job Scheduling (Databricks)**

Notebook job scheduling is a feature that allows you to **automate the
execution of notebooks** instead of running them manually. It helps
streamline data pipelines and improves efficiency.

**Key Features:**

- **Automatic Execution**

  - Schedule notebooks to run at specific times (e.g., daily at
    midnight).

  - Eliminates the need for manual intervention.

- **Trigger-Based Execution**

  - Run notebooks based on events (e.g., arrival of a file or external
    triggers).

- **Cluster Management**

  - Automatically starts the cluster, runs the notebook, and shuts it
    down.

  - No need to manually manage compute resources.

- **Monitoring & Logging**

  - Track past executions.

  - Easily monitor job status and logs.

- **Notifications**

  - Send alerts (email) on job start, success, or failure.

**How to Schedule a Notebook:**

1.  Click the **“Schedule”** button in the notebook.

2.  Define:

    - Job name (e.g., *EveryMidnight*)

    - Frequency (daily, etc.)

    - Time and timezone

3.  Choose or create a **cluster**.

4.  (Optional) Set up **notifications**.

5.  Click **Create**.

**Viewing Job Runs:**

- Go to **Compute → Job Compute** to see execution history and logs.

------------------------------------------------------------------------

**Bottom Line:**

Notebook scheduling in Databricks enables **automated, reliable, and
scalable data pipeline execution** with minimal manual effort.
