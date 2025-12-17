
# Dynamic Multi-Client ETL Pipeline

## 📖 Overview

This repository contains a robust, **production-grade PySpark ETL pipeline** designed to ingest, transform, and sessionize clickstream data from **MongoDB** into a partitioned **S3 Data Lake** (Parquet format).

It is built to handle multiple clients dynamically (e.g., *Gardenia*, *Kesari*) and performs complex **Gap-Based Sessionization** to enrich raw event logs with user session contexts.

## 🚀 Key Features

* **Dynamic Client Handling:** The script iterates through a configurable list of clients, applying standard transformation templates to each without code duplication.
* **Gap-Based Sessionization:** Implements a sophisticated "Time Gap" algorithm (window functions) to identify unique user sessions based on inactivity thresholds (> 30 minutes).
* **Incremental Processing:** Designed to run daily, processing only the previous day's data (`T-1`) to optimize performance and cost.
* **Historical Preservation:** Uses Spark's `partitionOverwriteMode=dynamic` to update only relevant date partitions without wiping historical data.
* **Data Enrichment:** Automatically joins derived Session IDs back to secondary tables (like `nudge_recommendations`, `fingerprint`) to provide a unified view of user activity.

## 🛠️ Architecture

```mermaid
flowchart TD
    MONGO[(MongoDB Replica Set)] -->|Read| SPARK(PySpark ETL Job)
    
    subgraph Transformation Logic
        SPARK --> CLEAN[Clean & Typecast]
        CLEAN --> SESSION{Is Session Table?}
        SESSION -->|Yes| GAP[Apply 30min Gap Logic]
        SESSION -->|No| LOOKUP[Join with Session Master]
        GAP --> ID[Generate Session IDs]
    end
    
    ID --> WRITE
    LOOKUP --> WRITE
    
    WRITE --> S3[(S3 Data Lake)]
    
    subgraph S3 Output Structure
        S3 --> CLIENT_A[Client: Gardenia]
        S3 --> CLIENT_B[Client: Kesari]
        CLIENT_A --> TABLES[Parquet Tables / Date Partitioned]
    end

```

## ⚙️ Configuration

### Global Settings

The script is controlled via the `GLOBAL CONFIGURATION` section at the top of the file:

| Constant | Description | Default |
| --- | --- | --- |
| `START_DATE` | Start of the data extraction window. | `Yesterday` (T-1) |
| `SPLIT_THRESHOLD_SECONDS` | Inactivity time required to trigger a new session ID. | `1800` (30 mins) |
| `CLIENTS` | List of client dictionaries mapping S3 folders to Mongo collections. | `[{'folder': 'gardenia' ...}]` |
| `STANDARD_TABLES` | Schemas defining how to map Mongo collections to S3 tables. | `activity_logs`, `user_sessions`, etc. |

### Environment Variables

The script attempts to load sensitive credentials from the environment, falling back to defaults if necessary.

```bash
export MONGO_URI="mongodb://user:password@host:port/db?authSource=admin"

```

## 🔄 Sessionization Logic (The "Gap" Algorithm)

The pipeline does not rely on pre-existing session IDs. Instead, it calculates them on the fly:

1. **Partition:** Data is grouped by unique user (`fingerprint`).
2. **Sort:** Events are ordered by time.
3. **Lag & Diff:** It calculates the time difference between the current event and the previous event.
4. **Threshold Check:**
* If `Time Diff > 30 mins`, a **New Session Flag** is raised.
* If `Time Diff <= 30 mins`, the event belongs to the **Current Session**.


5. **Cumulative Sum:** A running total of "New Session Flags" generates a unique `session_seq` for every user.

## 📂 Output Structure

Data is written to S3 in Snappy-compressed Parquet format, partitioned by `date`.

```text
s3://mongodatamprompt/etl/partitioned/
├── gardenia/
│   ├── user_sessions_v2/
│   │   └── date=2024-01-20/
│   │       └── part-0000.snappy.parquet
│   ├── activity_logs_v2/
│   └── nudge_recommendations_v2/
└── kesari/
    ├── user_sessions_v2/
    └── ...

```

## ⚠️ Operational Notes

* **Backfilling:** To re-process historical data, uncomment the `OPTION B: BACKFILL` line in the script and set your desired `START_DATE`.
* **Schema Evolution:** The script automatically flattens complex nested JSON fields (defined in `complex_cols`) into stringified JSON columns to prevent schema conflicts in Glue/Athena.
* **Cleanup:** The script includes a safety mechanism (`clean_s3_table_location`) which is currently **commented out** to prevent accidental data loss during incremental runs.

## 📋 Requirements

* **Spark 3.x**
* **MongoDB Spark Connector** (`com.mongodb.spark.sql.connector.MongoTableProvider`)
* **AWS CLI** (configured for S3 access)
