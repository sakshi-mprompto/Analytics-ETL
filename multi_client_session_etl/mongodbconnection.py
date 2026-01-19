import os
import logging
import json
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, to_timestamp, to_date, to_json, when, lag,
    sum as sum_, min as min_, max as max_, first,
    count as count_, concat_ws, sha2, year, month,
    dayofmonth, format_string, coalesce, lit, concat, round, broadcast, expr
)

# =============================================================================
# 1. INFRASTRUCTURE & CONFIGURATION
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger("kesari-golden-etl-prod")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:mPromptoAdminAi123@172.31.15.148:27017/analytics?authMechanism=SCRAM-SHA-256&authSource=admin")

DB = "analytics"
BASE_S3_PATH = "s3://mongodatamprompt/etl_data/partitioned/"
MONGO_PROVIDER = "com.mongodb.spark.sql.connector.MongoTableProvider"

# -----------------------------------------------------------------------------
# [CRITICAL] DATE CONFIGURATION
# -----------------------------------------------------------------------------
# OPTION A: REPAIR MODE (Use this NOW to fix the gap you just cleaned)
START_DATE = '2026-01-16' 
END_DATE = '2026-01-19'   

# OPTION B: DAILY SCHEDULE (Comment above & Uncomment below for normal runs)
# current_date = datetime.utcnow()
# START_DATE = (current_date - timedelta(days=1)).strftime('%Y-%m-%d')
# END_DATE = current_date.strftime('%Y-%m-%d')

log.info(f"📅 ETL JOB CONFIGURATION: Processing Data from {START_DATE} to {END_DATE}")

# [TUNING] Session splitting logic
SPLIT_THRESHOLD_SECONDS = 1800  # 30 Minutes of inactivity = New Session
JOIN_GRACE_PERIOD_SECONDS = 10  # Allow nudges to be 10s outside session bounds

# =============================================================================
# 2. SCHEMA DEFINITIONS
# =============================================================================
CLIENTS = [
    {"folder_name": "gardenia", "mongo_prefix": "gardenia"},
    {"folder_name": "kesari",   "mongo_prefix": "kesarishop"}
]

STANDARD_TABLES = [
    {
        "mongo_suffix": "activity_logs_v2",
        "target_name": "activity_logs_v2",
        "time_col": "time",
        "user_col": "fingerprint",
        # [FIX] Force 'details' to string prevents crashes when schema varies (Struct vs String)
        "force_mongo_string": ["details"],
        "complex_cols": [] 
    },
    {
        "mongo_suffix": "nudge_recommendations",
        "target_name": "nudge_recommendations_v2",
        "time_col": "createdAt",
        "user_col": "fingerprint",
        "complex_cols": ["recommendations", "qa_pairs"]
    },
    {
        "mongo_suffix": "nudge_responses_v2",
        "target_name": "nudge_responses_v2",
        "time_col": "createdAt",
        "user_col": "fingerprint",
        "complex_cols": []
    },
    {
        "mongo_suffix": "fingerprint",
        "target_name": "fingerprint_v2",
        "time_col": "createdAt",
        "user_col": "fingerprint",
        "complex_cols": ["userAgent"]
    },
    {
        "mongo_suffix": "user_sessions_v2",
        "target_name": "user_sessions_v2",
        "time_col": "sessionStart",
        "user_col": "fingerprint",
        "complex_cols": [],
        "is_session_table": True
    }
]

COLUMN_RENAMES = {
    "_id": "mongo_id",
    "sessionStart": "session_start",
    "sessionEnd": "session_end",
    "sessionend": "session_end",
    "createdAt": "created_at",
    "userAgent": "user_agent"
}

# =============================================================================
# 3. SPARK INITIALIZATION
# =============================================================================
spark = (
    SparkSession.builder
    .appName("Kesari_Golden_Session_ETL_PROD")
    # [OPTIMIZATION] Dynamic partition overwrite is safer for backfills, though we use Append mostly.
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)

# =============================================================================
# 4. HELPER FUNCTIONS
# =============================================================================
def get_mongo_pipeline(time_col, force_string_cols=None):
    """Generates MongoDB Aggregation Pipeline for server-side filtering"""
    pipeline = [
        {
            "$match": {
                time_col: {
                    "$gte": {"$date": f"{START_DATE}T00:00:00.000Z"},
                    "$lt":  {"$date": f"{END_DATE}T00:00:00.000Z"}
                }
            }
        }
    ]
    
    # [FIX] Handle schema drift by forcing fields to String on Mongo side
    if force_string_cols:
        conversions = {col_name: {"$toString": f"${col_name}"} for col_name in force_string_cols}
        pipeline.append({"$set": conversions})
        
    return json.dumps(pipeline)

# =============================================================================
# 5. CORE PROCESSOR
# =============================================================================
def process_table(client, table_cfg, session_lookup_df=None):
    source = f"{client['mongo_prefix']}_{table_cfg['mongo_suffix']}"
    target = f"{BASE_S3_PATH}{client['folder_name']}/{table_cfg['target_name']}/"
    
    log.info(f"--> Processing: {source}")

    # --- A. READ DATA ---
    try:
        pipeline = get_mongo_pipeline(
            table_cfg["time_col"], 
            table_cfg.get("force_mongo_string", [])
        )

        df = (
            spark.read.format(MONGO_PROVIDER)
            .option("connection.uri", MONGO_URI)
            .option("database", DB)
            .option("collection", source)
            .option("pipeline", pipeline)
            .load()
        )

    except Exception as e:
        log.error(f"    Read Error {source}: {e}")
        raise

    # Clean up JSON columns
    for c in table_cfg.get("complex_cols", []):
        if c in df.columns:
            df = df.withColumn(c, to_json(col(c)))

    # Rename standard columns
    for old, new in COLUMN_RENAMES.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    time_col = COLUMN_RENAMES.get(table_cfg["time_col"], table_cfg["time_col"])
    user_col = table_cfg["user_col"]

    df = (
        df.withColumn("event_timestamp", to_timestamp(col(time_col)))
          .withColumn("date", to_date(col("event_timestamp")))
          .filter(col("event_timestamp").isNotNull()) 
    )

    # --- B. SESSIONIZATION LOGIC ---
    if table_cfg.get("is_session_table"):
        log.info("    Applying Session Boundary Rules...")
        
        df = df.filter(col(user_col).isNotNull())
        
        # [CRITICAL FIX] Partition by DATE ensures sessions break at midnight.
        # This keeps data consistent whether running 1 day or 30 days.
        w_user = Window.partitionBy(user_col, "date").orderBy("event_timestamp")
        
        df = (
            df.withColumn("prev_ts", lag("event_timestamp").over(w_user))
              .withColumn("is_new_session", when(
                  col("prev_ts").isNull() | 
                  ((col("event_timestamp").cast("long") - col("prev_ts").cast("long")) > SPLIT_THRESHOLD_SECONDS),
                  1
              ).otherwise(0))
              .withColumn("session_seq", sum_("is_new_session").over(w_user))
        )

        w_seq = Window.partitionBy(user_col, "date", "session_seq")
        df = df.withColumn("session_start_marker", min_("event_timestamp").over(w_seq))
        
        # Generate Deterministic Session ID
        df = df.withColumn("session_id", sha2(concat_ws("||", 
            col(user_col), 
            col("session_start_marker").cast("string"), 
            col("session_seq").cast("string")
        ), 256))

        df = df.withColumn("session_label", concat(
            col(user_col), lit("__"), 
            col("session_start_marker").cast("string"), lit("__s"), 
            col("session_seq")
        ))

        df = df.withColumn("is_session_assigned", lit(True)) \
               .drop("prev_ts", "is_new_session", "session_seq", "session_start_marker")

    # --- C. ENRICHMENT (Join Events with Sessions) ---
    elif session_lookup_df is not None and user_col in df.columns:
        log.info("    Enriching with Grace-Period Join (Broadcast)...")
        
        # [OPTIMIZATION] Broadcast is fast for Lookup tables < 100MB
        df = df.join(
            broadcast(session_lookup_df),
            (df[user_col] == session_lookup_df["sess_user"]) &
            (df["event_timestamp"] >= session_lookup_df["sess_start_window"]) &
            (df["event_timestamp"] <= session_lookup_df["sess_end_window"]),
            "left"
        )
        
        df = df.withColumn("is_session_assigned", col("session_id").isNotNull()) \
               .drop("sess_user", "sess_start_window", "sess_end_window")

    # --- D. IDEMPOTENCY (Safe Re-runs) ---
    if "mongo_id" in df.columns:
        df = df.dropDuplicates(["mongo_id"])
    elif "session_id" in df.columns and table_cfg.get("is_session_table"):
        df = df.dropDuplicates(["session_id"])

    # --- E. WRITE TO S3 ---
    df = (
        df.withColumn("year", year(col("date")))
          .withColumn("month", format_string("%02d", month(col("date"))))
          .withColumn("day", format_string("%02d", dayofmonth(col("date"))))
    )
    
    # [OPTIMIZATION] Prevent "Small File Problem" in S3
    df = df.repartition("year", "month", "day")

    log.info(f"    Writing to S3 (APPEND): {target}")
    
    (
        df.write
        .mode("append") # [CRITICAL] Append prevents deleting history
        .partitionBy("year", "month", "day")
        .parquet(target)
    )
    
    return df

# =============================================================================
# 6. MASTER TABLE BUILDER
# =============================================================================
def build_session_master(client, session_df):
    if session_df is None: return

    target = f"{BASE_S3_PATH}{client['folder_name']}/session_master/"
    log.info(f"--> Building Session Master: {target}")

    master_df = (
        session_df
        .groupBy("session_id", "session_label", "fingerprint", "date")
        .agg(
            min_("event_timestamp").alias("session_start_ts"),
            # [CRITICAL FIX] Use actual 'session_end' from Mongo.
            # Using max(event_timestamp) caused the "Zero-Second Window" bug.
            max_(to_timestamp(col("session_end"))).alias("session_end_ts"),
            count_(lit(1)).alias("event_count")
        )
        .withColumn(
            "session_duration",
            round(col("session_end_ts").cast("double") - col("session_start_ts").cast("double"), 4)
        )
        .withColumn(
            "session_duration",
            when(col("session_duration") < 0, 0).otherwise(col("session_duration"))
        )
        .withColumn(
            "is_bounce", 
            (col("event_count") == 1) & (col("session_duration") < 10)
        )
        .withColumn("year", year(col("date")))
        .withColumn("month", format_string("%02d", month(col("date"))))
        .withColumn("day", format_string("%02d", dayofmonth(col("date"))))
    )

    master_df = master_df.dropDuplicates(["session_id"])
    master_df = master_df.repartition("year", "month", "day")
    master_df.write.mode("append").partitionBy("year", "month", "day").parquet(target)

# =============================================================================
# 7. EXECUTION ORCHESTRATOR
# =============================================================================
def main():
    log.info(f"=== KESARI GOLDEN ETL PROD START ===")
    
    for client in CLIENTS:
        client_name = client['folder_name']
        log.info(f"\n>>> CLIENT: {client_name} <<<")

        # STEP 1: Process Session Authority
        session_cfg = next(t for t in STANDARD_TABLES if t.get("is_session_table"))
        session_df = process_table(client, session_cfg)

        # STEP 2: Cache & Build Master
        if session_df is not None:
             session_df.cache()
             count = session_df.count()
             log.info(f"    Session DataFrame Cached. Total Sessions: {count}")
             
             build_session_master(client, session_df)
             
             # STEP 3: Create Optimized Lookup 
             # The window end must be calculated from 'session_end'.
             lookup = (
                session_df
                .groupBy("session_id")
                .agg(
                    first("fingerprint").alias("sess_user"),
                    (min_("event_timestamp") - expr(f"INTERVAL {JOIN_GRACE_PERIOD_SECONDS} SECONDS")).alias("sess_start_window"),
                    (max_(to_timestamp(col("session_end"))) + expr(f"INTERVAL {JOIN_GRACE_PERIOD_SECONDS} SECONDS")).alias("sess_end_window")
                )
             )
        else:
             lookup = None

        # STEP 4: Process All Other Event Tables
        for table in STANDARD_TABLES:
            if not table.get("is_session_table"):
                process_table(client, table, lookup)

        if session_df:
            session_df.unpersist()

    log.info("\n=== ETL COMPLETED SUCCESSFULLY ===")
    spark.stop()

if __name__ == "__main__":
    main()