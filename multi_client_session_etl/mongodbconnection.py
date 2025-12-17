"""
DYNAMIC MULTI-CLIENT ETL PIPELINE (Production Ready)
-----------------------------------------------------------
Features:
  - Gap-Based Sessionization (> 30 mins)
  - Dynamic Client List (Gardenia, Kesari)
  - Preserves Historical Data (No full folder wipe)
  - Fixed Session Duration Logic
"""

import sys
import subprocess
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, to_timestamp, to_date, to_json, lit, concat, 
    when, lag, sum, min as min_, max as max_, first
)

# ============================================================================
# 1. GLOBAL CONFIGURATION
# ============================================================================
current_date = datetime.now()

# OPTION A: PRODUCTION (Daily Incremental Run)
# This captures data for "Yesterday"
START_DATE = (current_date - timedelta(days=1)).strftime('%Y-%m-%d')

# OPTION B: BACKFILL (Uncomment this if you need to repair old data)
#START_DATE = '2025-12-13' 

# End at Today
END_DATE = current_date.strftime('%Y-%m-%d')
SPLIT_THRESHOLD_SECONDS = 1800 

# Mongo Connection
# Tries to get from Env Variable first, falls back to your hardcoded string
MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:mPromptoAdminAi123@172.31.15.148:27017/analytics?authMechanism=SCRAM-SHA-256&authSource=admin")
DB = "analytics"

# S3 Path
BASE_S3_PATH = "s3://mongodatamprompt/etl/partitioned/" 

# ============================================================================
# 2. CLIENT & TABLE CONFIGURATION
# ============================================================================
CLIENTS = [
    { "folder_name": "gardenia", "mongo_prefix": "gardenia" },
    { "folder_name": "kesari",   "mongo_prefix": "kesarishop" }
]

STANDARD_TABLES = [
    {
        "mongo_suffix": "activity_logs_v2",
        "target_name": "activity_logs_v2",
        "time_col": "time",
        "user_col": "fingerprint",
        "complex_cols": ["details"]
    },
    {
        "mongo_suffix": "fingerprint",
        "target_name": "fingerprint_v2", 
        "time_col": "createdAt",
        "user_col": "fingerprint",
        "complex_cols": ["userAgent"]
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
    "createdAt": "created_at",
    "userAgent": "user_agent",
    "productId": "product_id"
}

# ============================================================================
# 3. INITIALIZATION
# ============================================================================
spark = SparkSession.builder \
    .appName("Dynamic_Session_Enriched_ETL") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .getOrCreate() 

mongo_provider = "com.mongodb.spark.sql.connector.MongoTableProvider"

# ============================================================================
# 4. CORE FUNCTIONS
# ============================================================================

def clean_s3_table_location(s3_path):
    """
    WARNING: Deletes the entire folder. 
    Only use this if you want to wipe history.
    """
    print(f"    🗑️ Cleaning old data at: {s3_path}")
    try:
        cmd = ["aws", "s3", "rm", s3_path, "--recursive"]
        subprocess.run(cmd, capture_output=True, text=True, check=True)
    except Exception as e:
        print(f"    !!! AWS CLI Cleanup Error: {e}")

def remove_folder_markers(s3_path):
    """Removes Hadoop _$folder$ markers recursively."""
    try:
        cmd = ["aws", "s3", "rm", s3_path, "--recursive", "--exclude", "*", "--include", "*$folder$"]
        subprocess.run(cmd, capture_output=True)
    except Exception:
        pass


def process_single_table(client_config, table_template, session_lookup_df=None):
    source_collection = f"{client_config['mongo_prefix']}_{table_template['mongo_suffix']}"
    target_path = f"{BASE_S3_PATH}{client_config['folder_name']}/{table_template['target_name']}/"
    
    raw_time_col = table_template["time_col"]
    user_col = table_template["user_col"] 
    complex_cols = table_template.get("complex_cols", [])
    is_session_table = table_template.get("is_session_table", False)

    print(f"\n>>> PROCESSING: {client_config['folder_name'].upper()} -> {table_template['target_name']}")

    # 1. READ
    try:
        df = (spark.read
            .format(mongo_provider)
            .option("connection.uri", MONGO_URI)
            .option("database", DB)
            .option("collection", source_collection)
            .load()
        )
    except Exception:
        print(f"!!! Error reading {source_collection}. Skipping.")
        return None

    # 2. FLATTEN COMPLEX COLUMNS
    for col_name in complex_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, to_json(col(col_name)))

    # 3. RENAME COLUMNS
    for old_name, new_name in COLUMN_RENAMES.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    current_time_col = COLUMN_RENAMES.get(raw_time_col, raw_time_col)
    
    if current_time_col not in df.columns:
        return None

    df = df.withColumn("event_timestamp", to_timestamp(col(current_time_col)))
    df = df.withColumn("date", to_date(col("event_timestamp")))

    # 5. FILTER
    df_filtered = df.filter((col("date") >= START_DATE) & (col("date") <= END_DATE))
    
    if df_filtered.isEmpty():
        print(f"    No data found in date range.")
        return None

    df_final = df_filtered

    # ========================================================================
    # 6A. SESSION TABLE LOGIC (GAP-BASED SESSIONIZATION)
    # ========================================================================
    if is_session_table and user_col in df_final.columns:
        print(f"    --> [MASTER] Generating Session IDs (Gap > {SPLIT_THRESHOLD_SECONDS}s)...")
        
        # 1. Define Window for Gap Detection
        w = Window.partitionBy(user_col).orderBy("event_timestamp")
        
        # 2. Get Previous Timestamp
        df_final = df_final.withColumn("prev_ts", lag("event_timestamp").over(w))
        
        # 3. Calculate Time Difference (Gap)
        df_final = df_final.withColumn("time_diff", 
            col("event_timestamp").cast("long") - col("prev_ts").cast("long")
        )
        
        # 4. Flag New Session
        df_final = df_final.withColumn("is_new_session", 
            when(col("prev_ts").isNull() | (col("time_diff") > SPLIT_THRESHOLD_SECONDS), 1).otherwise(0)
        )
        
        # 5. Generate Session Sequence (Cumulative Sum)
        df_final = df_final.withColumn("session_seq", sum("is_new_session").over(w))
        
        # 6. Create Unique Session ID
        df_final = df_final.withColumn("session_id", 
            concat(col(user_col), lit("_"), col("session_seq").cast("string"))
        )
        
        # 7. Cleanup & Basic Duration (Row level)
        # We ensure session_end is a valid timestamp
        if "session_end" in df_final.columns:
            df_final = df_final.withColumn("session_end_ts", to_timestamp(col("session_end")))
        else:
            # Fallback if no end time exists (default 5 mins)
            df_final = df_final.withColumn("session_end_ts", 
                (col("event_timestamp").cast("long") + 300).cast("timestamp")
            )
        
        # ---------------------------------------------------------
        # DURATION CALCULATION
        # ---------------------------------------------------------
        df_final = df_final.withColumn("session_duration", 
            col("session_end_ts").cast("long") - col("event_timestamp").cast("long")
        )

        df_final = df_final.drop("prev_ts", "time_diff", "is_new_session", "session_seq")

    # ========================================================================
    # 6B. OTHER TABLES (JOIN)
    # ========================================================================
    elif session_lookup_df is not None and user_col in df_final.columns:
        print(f"    --> [ENRICH] Joining with Session Master...")
        
        cond = [
            df_final[user_col] == session_lookup_df["sess_user"],
            df_final["event_timestamp"] >= session_lookup_df["sess_start"],
            df_final["event_timestamp"] <= session_lookup_df["sess_end"]
        ]
        
        df_joined = df_final.join(session_lookup_df, cond, "left")
        df_final = df_joined.drop("sess_user", "sess_start", "sess_end")

    # 7. WRITE
    row_count = df_final.count()
    print(f"    Writing {row_count} rows...")
    
    try:
        # ⭐ FIXED: COMMENTED OUT TO PREVENT DELETION OF HISTORICAL DATA
        # clean_s3_table_location(target_path) 
        
        (df_final.write
            .mode("overwrite")
            .partitionBy("date")
            .parquet(target_path)
        )
        print(f"    ✔ FINISHED")
        
        # It is safe to remove folder markers after writing
        remove_folder_markers(target_path)

    except Exception as e:
        print(f"!!! Write Error: {str(e)}")
        
    return df_final

# ============================================================================
# 5. EXECUTION LOGIC
# ============================================================================
print(f"Starting Dynamic ETL with Gap-Based Sessionization (Start: {START_DATE} | End: {END_DATE})...")

for client in CLIENTS:
    print(f"\n{'='*60}\nCLIENT: {client['folder_name']}\n{'='*60}")
    
    # PHASE 1: SESSION MASTER
    session_template = next((t for t in STANDARD_TABLES if t.get("is_session_table")), None)
    session_lookup_df = None
    
    if session_template:
        print("\n--- PHASE 1: Processing Session Master ---")
        processed_session_df = process_single_table(client, session_template)
        
        if processed_session_df:
            # AGGREGATE to get strict Session Boundaries for the Lookup Table
            print("    [Aggregation] Calculating Session Boundaries for Lookup...")
            
            session_lookup_df = processed_session_df.groupBy("session_id").agg(
                first(col("fingerprint")).alias("sess_user"),
                min_("event_timestamp").alias("sess_start"),
                (max_("event_timestamp").cast("long") + SPLIT_THRESHOLD_SECONDS).cast("timestamp").alias("sess_end")
            ).cache()
            
            print("    [Cache] Session Map cached.")

    # PHASE 2: OTHERS
    print("\n--- PHASE 2: Processing & Enriching Other Tables ---")
    for template in STANDARD_TABLES:
        if template.get("is_session_table"): continue
        process_single_table(client, template, session_lookup_df)
        
    if session_lookup_df: session_lookup_df.unpersist()

# --- FINAL GLOBAL CLEANUP ---
print("\n" + "="*60)
bucket_name = BASE_S3_PATH.split("/")[2]
bucket_root = f"s3://{bucket_name}/"
print(f"🧹 Performing final cleanup of ALL $folder$ markers in bucket: {bucket_root}")
remove_folder_markers(bucket_root)
print("="*60)

print("\n\nAll clients processed successfully.")
spark.stop()