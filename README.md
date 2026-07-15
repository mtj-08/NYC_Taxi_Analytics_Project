# NYC Taxi Analytics 

This repository contains two end-to-end implementations of the same analytics use case — NYC Yellow Taxi trip data — built on **Microsoft Fabric**. Both pipelines consume the same NYC TLC Parquet source files and deliver the same Power BI reporting layer, but they differ fundamentally in their architectural philosophy, tooling, and transformation approach.

| | BI Pipeline | Data Engineering Pipeline |
|---|---|---|
| **Architecture** | Staging → Presentation | Medallion (Bronze → Silver → Gold) |
| **Processing Engine** | T-SQL + Dataflow Gen2 | PySpark Notebooks |
| **Storage** | Fabric Data Warehouse | Azure Data Lake Storage Gen2 |
| **Transformation Style** | Stored Procedures + Power Query | Distributed in-memory compute |
| **Format** | Relational tables | Delta Lake (Parquet + transaction log) |

---

## Table of Contents

1. [Repository Structure](#repository-structure)
2. [Data Source](#data-source)
3. [BI Pipeline — Microsoft Fabric Warehouse](#bi-pipeline--microsoft-fabric-warehouse)
   - [Architecture Overview](#bi-architecture-overview)
   - [Stage 1 — Staging Ingestion](#stage-1--staging-ingestion)
   - [Stage 2 — Presentation Pipeline](#stage-2--presentation-pipeline)
   - [Stage 3 — Master Pipeline](#stage-3--master-pipeline)
   - [Metadata Tracking](#bi-metadata-tracking)
   - [Semantic Model & Power BI Report](#semantic-model--power-bi-report)
4. [Data Engineering Pipeline — Medallion Architecture](#data-engineering-pipeline--medallion-architecture)
   - [Architecture Overview](#de-architecture-overview)
   - [Bronze Layer — Raw Ingestion](#bronze-layer--raw-ingestion)
   - [Silver Layer — Cleansed & Conformed](#silver-layer--cleansed--conformed)
   - [Gold Layer — Aggregated & Serving](#gold-layer--aggregated--serving)
5. [Concepts Explained](#concepts-explained)
   - [BI Concepts](#bi-concepts)
   - [Data Engineering Concepts](#data-engineering-concepts)
6. [Key Design Decisions & Comparisons](#key-design-decisions--comparisons)
7. [Setup & Execution](#setup--execution)

---

## Repository Structure

```
nyc-taxi-analytics/
│
├── bi-pipeline/
│   ├── pipelines/
│   │   ├── nyc_yellow_ingestion_pipeline    # Copy + clean + metadata (staging)
│   │   ├── nyc_presentation_pipeline        # Dataflow Gen2 + metadata (presentation)
│   │   └── nyc_master_pipeline              # Invokes both in sequence
│   ├── stored_procedures/
│   │   ├── STG.nyc_yellow_datacleaning.sql
│   │   ├── metadata.processing_stProc.sql
│   │   └── metadata.insert_presentation_metadata.sql
│   └── tables/
│       ├── metadata.processing_log.sql
│       └── dbo.nyctaxi_yellow_proc.sql
│
├── de-pipeline/
│   ├── notebooks/
│   │   ├── 01_bronze_ingestion.ipynb        # Raw parquet → ADLS Bronze
│   │   ├── 02_silver_transformation.ipynb   # Cleansing + conforming → Silver Delta
│   │   └── 03_gold_aggregation.ipynb        # Business aggregates → Gold Delta
│   ├── pipelines/
│   │   └── nyc_medallion_master_pipeline    # Orchestrates all three notebooks
│   └── utils/
│       └── schema_definitions.py
│
└── README.md
```

---

## Data Source

**NYC TLC Yellow Taxi Trip Records** — monthly Parquet files publicly available from the NYC Taxi & Limousine Commission.

- Source: [https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Format: `.parquet`, one file per month
- Naming convention: `yellow_tripdata_YYYY-MM.parquet`
- Coverage: Trip-level records including pickup/dropoff timestamps, locations, fares, distances, passenger counts, and payment types

Both pipelines consume these same files. The BI pipeline reads them into a Fabric Data Warehouse; the DE pipeline lands them raw into ADLS Gen2.

---

## BI Pipeline — Microsoft Fabric Warehouse

### BI Architecture Overview

The BI pipeline follows a classic **Staging → Presentation** two-layer warehouse pattern. Data is first copied raw into a staging schema, cleaned in-place via a stored procedure, then promoted to a presentation layer through a Dataflow Gen2 transformation. A metadata table tracks every run for both audit and incremental load purposes.

```
Parquet Files (NYC TLC)
        ↓
[Copy Data Activity] — dynamic filename via concat + Date variable
        ↓
STG.NYC_Taxi_Yellow  ← Staging Layer (Fabric Data Warehouse)
        ↓
[Stored Procedure] — removes out-of-window pickup records
        ↓
[Dataflow Gen2] — column cleanup, borough/zone join, conditional columns
        ↓
dbo.nyctaxi_yellow_proc  ← Presentation Layer
        ↓
Semantic Model → Power BI Report
```

Metadata is written to `metadata.processing_log` after both the staging load and the presentation load.

---

### Stage 1 — Staging Ingestion

**Copy Data Activity**

The Copy Data activity reads the monthly Parquet file from the NYC TLC source and loads it directly into `STG.NYC_Taxi_Yellow` in the Fabric Data Warehouse. No schema transformation happens at this step — the data lands as-is, preserving the source fidelity.

The filename is constructed dynamically at runtime:

```
@concat('yellow_tripdata_', variables('Date'), '.parquet')
```

This means no hardcoded file path exists anywhere in the pipeline. Every run computes the correct filename from the `Date` variable.

**Dynamic Date Allocation (Script Activity)**

Rather than specifying which month to load manually, the pipeline queries the metadata log to determine the last successfully processed month:

```sql
SELECT TOP 1 latest_processed_pickup
FROM metadata.processing_log
WHERE table_processed = 'NYC_Taxi_Yellow'
ORDER BY latest_processed_pickup DESC;
```

The result is incremented by one month and formatted as `yyyy-MM` using a Set Variable activity:

```
@formatDateTime(
  addToTime(
    activity('DynamicDateAllocation').output.resultSets[0].rows[0].latest_processed_pickup,
    1, 'Month'
  ),
  'yyyy-MM'
)
```

This self-driving date logic means the pipeline is fully autonomous — run it each month and it will always load the correct next file.

**Data Cleaning — Stored Procedure**

Once the raw data is staged, a stored procedure removes records whose `tpep_pickup_datetime` falls outside the valid window for that month. This catches a well-known data quality issue in the NYC TLC dataset where trip records from other months bleed into a given file.

```sql
CREATE PROCEDURE STG.nyc_yellow_datacleaning
    @start_date DATETIME2,
    @end_date   DATETIME2
AS
    DELETE FROM STG.NYC_Taxi_Yellow
    WHERE tpep_pickup_datetime < @start_date
       OR tpep_pickup_datetime > @end_date;
```

The `@end_date` is dynamically computed within the pipeline:

```
@addToTime(concat(variables('Date'), '-01'), 1, 'Month')
```

**Metadata Logging — Stored Procedure**

After a successful load, pipeline metadata is written to the central log:

```sql
CREATE PROCEDURE metadata.processing_stProc
    @pipeline_run_id VARCHAR(255),
    @table_name      VARCHAR(255),
    @processed_date  DATETIME2
AS
    INSERT INTO metadata.processing_log
      (pipeline_run_id, table_processed, rows_processed,
       latest_processed_pickup, processed_datetime)
    SELECT
        @pipeline_run_id,
        @table_name,
        COUNT(*),
        MAX(tpep_pickup_datetime),
        @processed_date
    FROM STG.NYC_Taxi_Yellow;
```

---

### Stage 2 — Presentation Pipeline

**Dataflow Gen2**

Dataflow Gen2 reads from the staging schema and performs the following transformations using Power Query (M language under the hood):

- Removes columns not required for reporting (e.g. internal rate codes, store-and-forward flags)
- Adds a conditional column for business categorisation (e.g. short vs long trips, peak vs off-peak)
- Left-joins the NYC Taxi Zone lookup table to enrich each record with pickup borough/zone and dropoff borough/zone
- Writes the enriched dataset to `dbo.nyctaxi_yellow_proc`

**Presentation Table Schema**

```sql
CREATE TABLE dbo.nyctaxi_yellow_proc (
    vendor                  VARCHAR(50),
    tpep_pickup_datetime    DATE,
    tpep_dropoff_datetime   DATE,
    pu_borough              VARCHAR(100),
    pu_zone                 VARCHAR(100),
    do_borough              VARCHAR(100),
    do_zone                 VARCHAR(100),
    payment_method          VARCHAR(50),
    passenger_count         INT,
    trip_distance           FLOAT,
    total_amount            FLOAT
);
```

**Metadata Logging — Presentation Layer**

A second stored procedure logs the presentation load separately, providing a complete audit trail for both layers:

```sql
CREATE PROCEDURE metadata.insert_presentation_metadata
    @pipeline_run_id VARCHAR(255),
    @table_name      VARCHAR(255),
    @processed_date  DATETIME2
AS
    INSERT INTO metadata.processing_log
      (pipeline_run_id, table_processed, rows_processed,
       latest_processed_pickup, processed_datetime)
    SELECT
        @pipeline_run_id,
        @table_name,
        COUNT(*),
        MAX(tpep_pickup_datetime),
        @processed_date
    FROM dbo.nyctaxi_yellow;
```

---

### Stage 3 — Master Pipeline

A parent pipeline called `nyc_master_pipeline` invokes both the ingestion and presentation pipelines in sequence via **Execute Pipeline** activities. This provides a single trigger point for the full end-to-end run. If only the staging layer needs to be rerun (e.g. to reload a specific month), each child pipeline can be triggered independently.

<img width="940" height="275" alt="image" src="https://github.com/user-attachments/assets/b3e7f8cc-7f71-42ca-91d9-f9decceed851" />

---

### BI Metadata Tracking

All pipeline runs across both layers are recorded in a single central table:

```sql
CREATE TABLE metadata.processing_log (
    pipeline_run_id         VARCHAR(255),
    table_processed         VARCHAR(255),
    rows_processed          INT,
    latest_processed_pickup DATETIME2(6),
    processed_datetime      DATETIME2(6)
);
```

This table serves a dual purpose:

- **Audit trail** — every run is stamped with a pipeline run ID, row count, and timestamp, making it easy to diagnose failures or investigate data anomalies
- **Incremental load driver** — the `latest_processed_pickup` column is queried at the start of each run to automatically determine which month to load next

---

### Semantic Model & Power BI Report

Once data lands in `dbo.nyctaxi_yellow_proc`, it is published as a **Fabric Semantic Model** (formerly known as a Power BI Dataset). A report is created directly from the semantic model within the Fabric workspace.

The report delivers analytics across:

- Trip volume trends over time
- Revenue and fare analysis by period
- Pickup and dropoff borough and zone breakdowns
- Passenger count and trip distance distributions
- Payment method splits (card vs cash vs other)

The presentation layer is designed to be **DirectLake-ready**, meaning Power BI can query it without a traditional import or DirectQuery round-trip, enabling fast report refresh at scale.

<img width="940" height="505" alt="image" src="https://github.com/user-attachments/assets/ec67be53-d1bb-44df-887f-addc5a10e1de" />

---

## Data Engineering Pipeline — Medallion Architecture

### DE Architecture Overview

The DE pipeline follows the **Medallion (Multi-hop) Architecture**, a modern data lakehouse pattern that organises data into three progressively refined layers stored in Azure Data Lake Storage Gen2 as Delta Lake tables. PySpark notebooks running on Fabric's Spark compute handle all transformations. The data from the Azure storage container is accessed using the fabric shortcut.
<img width="940" height="323" alt="image" src="https://github.com/user-attachments/assets/b7a0a15e-9e30-434e-883b-609d66f9be0f" />


```
Parquet Files (NYC TLC)
        ↓
[Bronze Notebook] — raw ingest to ADLS Gen2, no transformation
        ↓
ADLS Gen2: /bronze/nyc_taxi/yellow/YYYY/MM/  ← Bronze Layer (Delta)
        ↓
[Silver Notebook] — schema enforcement, null handling, outlier removal, type casting
        ↓
ADLS Gen2: /silver/nyc_taxi/yellow/           ← Silver Layer (Delta, partitioned)
        ↓
[Gold Notebook] — business aggregations, zone enrichment, KPI tables
        ↓
ADLS Gen2: /gold/nyc_taxi/                    ← Gold Layer (Delta, serving)
        ↓
Semantic Model → Power BI Report
```

All three notebooks are orchestrated by a single master pipeline that executes them in order.

<img width="940" height="421" alt="image" src="https://github.com/user-attachments/assets/ec49a570-492e-4103-bd01-fc41d472324f" />

---

### Bronze Layer — Raw Ingestion

**Purpose:** Preserve the source data exactly as received. The Bronze layer is an immutable, append-only record of every file ingested. No business logic, no filtering, no schema changes.

**What happens here:**

The Bronze notebook reads the monthly Parquet file from the NYC TLC source and writes it to ADLS Gen2 as a Delta table, partitioned by year and month:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read raw parquet from source
df_raw = spark.read.parquet(f"abfss://raw@<storage>.dfs.core.windows.net/yellow_tripdata_{year_month}.parquet")

# Write to bronze as Delta, partitioned for efficient downstream reads
df_raw.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("year", "month") \
    .save("abfss://bronze@<storage>.dfs.core.windows.net/nyc_taxi/yellow/")
```

**Why Delta format?** Delta Lake adds a transaction log (the `_delta_log` folder) on top of Parquet files. This gives the Bronze layer ACID guarantees, time travel (the ability to query historical versions), and schema enforcement — features that a plain Parquet landing zone lacks.

---

### Silver Layer — Cleansed & Conformed

**Purpose:** Produce a clean, typed, validated dataset that analysts and downstream processes can trust. The Silver layer contains all raw records minus noise, with consistent data types and a stable schema.

**What happens here:**

```python
from pyspark.sql.functions import col, to_date, when, lit

df_bronze = spark.read.format("delta").load("abfss://bronze@<storage>.dfs.core.windows.net/nyc_taxi/yellow/")

df_silver = (
    df_bronze
    # Cast types
    .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp"))
    .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))
    # Remove out-of-window records (same outlier logic as BI stored procedure)
    .filter(
        (col("tpep_pickup_datetime") >= lit(start_date)) &
        (col("tpep_pickup_datetime") < lit(end_date))
    )
    # Drop nulls in critical fields
    .dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "total_amount"])
    # Remove nonsensical values
    .filter(col("trip_distance") > 0)
    .filter(col("total_amount") > 0)
    .filter(col("passenger_count") > 0)
    # Standardise vendor names
    .withColumn("vendor", when(col("VendorID") == 1, "Creative Mobile").otherwise("Curb"))
    # Derive date column for partitioning
    .withColumn("pickup_date", to_date("tpep_pickup_datetime"))
)

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("pickup_date") \
    .save("abfss://silver@<storage>.dfs.core.windows.net/nyc_taxi/yellow/")
```

**Schema enforcement** is handled by Delta Lake. If the Bronze schema drifts (e.g. a new column appears in a future TLC file), the Silver notebook surfaces the mismatch explicitly rather than silently loading corrupt data.

**Partitioning by `pickup_date`** means Power BI and downstream notebooks only scan the partitions they need, dramatically reducing query costs on large datasets.

---

### Gold Layer — Aggregated & Serving

**Purpose:** Produce business-ready, pre-aggregated tables optimised for reporting. The Gold layer is what the Semantic Model reads. By pre-aggregating here, Power BI visuals render faster and the semantic model remains simple.

**What happens here:**

```python
from pyspark.sql.functions import col, sum, avg, count, round

df_silver = spark.read.format("delta").load("abfss://silver@<storage>.dfs.core.windows.net/nyc_taxi/yellow/")
df_zones  = spark.read.option("header", True).csv("abfss://reference@<storage>.dfs.core.windows.net/taxi_zone_lookup.csv")

# Join borough/zone lookup (same enrichment as BI Dataflow Gen2)
df_enriched = (
    df_silver
    .join(df_zones.withColumnRenamed("LocationID", "PULocationID")
                  .withColumnRenamed("Borough", "pu_borough")
                  .withColumnRenamed("Zone", "pu_zone"),
          on="PULocationID", how="left")
    .join(df_zones.withColumnRenamed("LocationID", "DOLocationID")
                  .withColumnRenamed("Borough", "do_borough")
                  .withColumnRenamed("Zone", "do_zone"),
          on="DOLocationID", how="left")
)

# Daily aggregation by zone pair
df_gold = (
    df_enriched
    .groupBy("pickup_date", "pu_borough", "pu_zone", "do_borough", "do_zone", "payment_method", "vendor")
    .agg(
        count("*").alias("trip_count"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("trip_distance"), 2).alias("avg_distance"),
        round(avg("passenger_count"), 2).alias("avg_passengers")
    )
)

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://gold@<storage>.dfs.core.windows.net/nyc_taxi/trips_daily_summary/")
```

The Gold layer may contain multiple tables (e.g. a daily summary, a borough-level rollup, a payment method breakdown), each optimised for a specific reporting need.

**Event Trigger

An Azure Blob Storage Event trigger has been created to automated the process. The trigger rule was assigned in such a way that whenever a file is uploaded in the ADLS Gen2 container, the pipeline gets triggered.

<img width="940" height="377" alt="image" src="https://github.com/user-attachments/assets/567c1e34-161b-4d5d-b54b-6435a43d9e63" />

---

## Concepts Explained

### BI Concepts

**Staging → Presentation Pattern**

A two-layer relational warehouse pattern where raw data lands in a staging schema (sometimes called landing or raw), undergoes cleaning and transformation, then is promoted into a presentation schema that is optimised for reporting. Staging tables are transient and can be truncated and reloaded; presentation tables are stable and serve the semantic model.

**Stored Procedures**

Reusable, parameterised T-SQL routines stored inside the database engine. In this project, stored procedures handle data cleaning (deleting out-of-window records) and metadata logging. Using stored procedures rather than ad-hoc SQL in pipeline scripts makes logic version-controllable, testable, and reusable across months without modification.

**Dataflow Gen2 (Power Query)**

A low-code ETL tool in Microsoft Fabric that uses the Power Query M language under the hood. It connects to sources, applies transformations visually or via formula, and writes outputs to destinations — in this case from the staging schema to the presentation table. It is particularly efficient for column mapping, conditional logic, and join operations that would otherwise require significant T-SQL.

**Semantic Model (formerly Power BI Dataset)**

A reusable, centralised definition of the data available to Power BI reports. It defines tables, relationships, measures (DAX calculations), and hierarchies. By building reports on top of a semantic model rather than directly on the warehouse table, multiple reports can share the same business logic and remain consistent.

**Incremental Load Pattern**

Instead of reloading all historical data on every run, an incremental pipeline identifies what is new since the last run and loads only that. Here, the `metadata.processing_log` table records the latest processed pickup datetime, and each new run adds exactly one month of new data. This keeps pipeline runtime and warehouse costs constant regardless of how many months of history accumulate.

**Dynamic Expressions in Data Pipelines**

Fabric Data Pipelines support expression language (similar to Azure Data Factory expressions) for building values at runtime: `@concat(...)`, `@formatDateTime(...)`, `@addToTime(...)`, `@variables(...)`. Using these instead of hardcoded strings means a single pipeline definition handles every monthly run without any changes.

**Metadata Logging**

Writing pipeline execution details (run ID, table name, row count, timestamps) to a dedicated metadata table after each pipeline run. This serves as an audit trail for debugging failures, validating row counts, and driving incremental load logic.

---

### Data Engineering Concepts

**Medallion Architecture (Multi-hop)**

A layered lakehouse design pattern where data flows through Bronze, Silver, and Gold layers, each adding a level of refinement:

- **Bronze** — raw, immutable, append-only copy of the source. Never modified after ingestion.
- **Silver** — cleaned, typed, validated. Suitable for ad-hoc analysis and feature engineering.
- **Gold** — aggregated, business-oriented. Optimised for serving dashboards and reports.

This separation of concerns means a data quality issue found in Silver does not require re-ingesting from the source — only the Silver and Gold layers need to be reprocessed.

**Delta Lake**

An open-source storage layer that brings ACID transactions, scalable metadata handling, and time travel to data lakes. Delta tables are Parquet files with an accompanying `_delta_log` folder that records every write operation as a JSON commit. Key capabilities used in this project:

- **ACID transactions** — writes either fully succeed or fully fail; no partial writes that corrupt the table
- **Schema enforcement** — rejects writes that don't match the defined schema
- **Time travel** — query a previous version of the table using `VERSION AS OF` or `TIMESTAMP AS OF`
- **Merge / upsert** — efficiently update existing records without rewriting the whole table

**PySpark**

The Python API for Apache Spark, a distributed in-memory data processing engine. PySpark DataFrames allow transformations to be expressed as high-level operations (filter, join, groupBy, withColumn) that Spark compiles into an optimised execution plan and distributes across a cluster. In this project, PySpark handles the same cleaning and enrichment logic that T-SQL and Power Query handle in the BI pipeline, but operates on files in ADLS rather than rows in a relational database.

**Azure Data Lake Storage Gen2 (ADLS Gen2)**

A cloud object storage service (built on Azure Blob Storage) with a hierarchical namespace, designed for big data analytics workloads. ADLS Gen2 supports Spark, Hive, and Delta Lake natively through the `abfss://` protocol. Data is organised in containers and virtual folders — in this project: `bronze/`, `silver/`, `gold/`, and `reference/`.

**Partitioning**

Dividing a large table into smaller physical chunks based on the values of a column. Spark reads only the relevant partitions when a query filters on the partition column, skipping the rest (partition pruning). In the Silver layer, partitioning by `pickup_date` means a query for a single month reads only that month's files, not the entire dataset. In Bronze, partitioning by `year` and `month` similarly scopes reads.

**Schema Enforcement vs Schema Evolution**

Delta Lake enforces the schema defined at table creation by default — writing a DataFrame with unexpected columns or wrong types raises an error. This is intentional: it prevents silent data corruption. Schema evolution (adding new columns) is opt-in via `.option("mergeSchema", "true")`. In this pipeline, schema enforcement acts as an early warning system if the NYC TLC source file format changes.

**Notebook Orchestration via Fabric Pipelines**

PySpark notebooks are individual executable units. Connecting them into a sequence (Bronze → Silver → Gold) is done via a Fabric Data Pipeline that uses Execute Notebook activities in order. Parameters (e.g. the year/month to process) are passed from the pipeline into each notebook at runtime, keeping the notebook code generic and reusable.

---

## Key Design Decisions & Comparisons

| Decision | BI Pipeline | DE Pipeline | Rationale |
|---|---|---|---|
| **Transformation engine** | T-SQL stored procedures + Dataflow Gen2 | PySpark notebooks | SQL is native to relational warehouses; PySpark scales to lakehouse workloads |
| **Storage format** | Relational tables in Fabric DW | Delta Lake on ADLS Gen2 | Relational tables support fast SQL queries; Delta supports distributed Spark reads at scale |
| **Outlier removal** | Stored procedure with `DELETE` | PySpark `.filter()` | Both achieve the same outcome; DELETE modifies in-place, filter creates a new clean DataFrame |
| **Zone enrichment** | Left join in Dataflow Gen2 | Left join in PySpark | Identical logic, different execution environment |
| **Incremental loading** | Metadata table + dynamic date variable | Metadata table or pipeline parameters | Both are self-driving; DE pipeline can also accept explicit parameters for backfill |
| **Schema governance** | Enforced by SQL `CREATE TABLE` DDL | Enforced by Delta Lake schema enforcement | Both prevent schema drift; Delta additionally supports time travel |
| **Serving layer** | DirectLake on `dbo.nyctaxi_yellow_proc` | DirectLake on Gold Delta table | Both feed the same Power BI semantic model |
| **Observability** | `metadata.processing_log` table | `metadata.processing_log` table | Shared pattern across both pipelines |

**Why both pipelines in one repo?**

The BI pipeline is optimised for organisations already invested in SQL and Power Query skills — it is approachable, maintainable by analysts, and tightly integrated with the Fabric Data Warehouse. The DE pipeline is optimised for large-scale, distributed processing and is more aligned with a data engineering team comfortable with Python and Spark. Having both in a single repository demonstrates the same analytical outcome can be achieved via different architectural patterns, and allows direct comparison of design choices, code complexity, and tooling trade-offs.

---

## Setup & Execution

### Prerequisites (Both Pipelines)

- Microsoft Fabric workspace
- Access to NYC TLC Yellow Taxi Parquet files
- For the DE pipeline: ADLS Gen2 storage account with containers `bronze`, `silver`, `gold`, and `reference` pre-created

---

### BI Pipeline Setup

1. Create schemas in the Fabric Data Warehouse:
   ```sql
   CREATE SCHEMA STG;
   CREATE SCHEMA metadata;
   ```

2. Run `metadata.processing_log.sql` and `dbo.nyctaxi_yellow_proc.sql` to create tables.

3. Deploy all stored procedures from the `stored_procedures/` folder.

4. Configure the ingestion pipeline — wire up the Copy Data activity with the dynamic filename expression, Set Variable activity, and stored procedure activities.

5. Configure the Dataflow Gen2 — connect to `STG.NYC_Taxi_Yellow`, apply transformations, and set the output destination to `dbo.nyctaxi_yellow_proc`.

6. Configure the master pipeline with Execute Pipeline activities pointing to the ingestion and presentation pipelines.

7. Seed the metadata log with the month before your first load:
   ```sql
   INSERT INTO metadata.processing_log (table_processed, latest_processed_pickup)
   VALUES ('NYC_Taxi_Yellow', '2023-12-31');
   ```

8. Trigger `nyc_master_pipeline`. It will automatically load January 2024 on the first run, then February on the next, and so on.

9. Publish the semantic model from `dbo.nyctaxi_yellow_proc` and create a Power BI report in the Fabric workspace.

---

### DE Pipeline Setup

1. Create ADLS Gen2 containers: `bronze`, `silver`, `gold`, `reference`.

2. Upload the NYC TLC taxi zone lookup CSV to `reference/taxi_zone_lookup.csv`.

3. Upload target Parquet files to a staging location or point the Bronze notebook directly to the TLC URL.

4. Configure Fabric Spark settings — ensure the Lakehouse is attached to the Spark environment and the ADLS mount or `abfss://` path credentials are configured.

5. Open `01_bronze_ingestion.ipynb` and set the `year_month` parameter (e.g. `2024-01`).

6. Deploy all three notebooks to the Fabric workspace.

7. Configure `nyc_medallion_master_pipeline` with Execute Notebook activities in order: Bronze → Silver → Gold. Pass `year_month` as a pipeline parameter to each notebook.

8. Trigger the master pipeline. For initial loads or backfills, run the pipeline once per month passing the relevant `year_month` value.

9. Point the Fabric Semantic Model to the Gold Delta table and create the Power BI report.

---

*Both pipelines deliver the same Power BI report — the architecture underneath is the choice of implementation style, scale requirements, and team skill set.*
