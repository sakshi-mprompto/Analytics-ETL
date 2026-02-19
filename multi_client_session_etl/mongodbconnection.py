import os
import logging
import json
import boto3
from urllib.parse import urlparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, to_timestamp, to_date, to_json, when, lag,
    sum as sum_, min as min_, max as max_, first,
    count as count_, concat_ws, sha2, year, month,
    dayofmonth, format_string, coalesce, lit, concat, round, expr,
    row_number, abs as abs_, broadcast
)

# =============================================================================
# 1. INFRASTRUCTURE & CONFIGURATION
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("kesari-golden-etl-prod")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:mPromptoAdminAi123@172.31.15.148:27017/analytics?authMechanism=SCRAM-SHA-256&authSource=admin")


DB = "analytics"
BASE_S3_PATH = "s3a://mongodatamprompt/etl_data/partitioned/"
MONGO_PROVIDER = "com.mongodb.spark.sql.connector.MongoTableProvider"

# =============================================================================
# --- DYNAMIC DATE CONFIGURATION (Yesterday Only) ---
# COMMENT THESE OUT FOR RESTORE:
current_date = datetime.utcnow()
START_DATE_STR = (current_date - timedelta(days=1)).strftime("%Y-%m-%d")
NEXT_DAY_STR = current_date.strftime("%Y-%m-%d")

# --- MANUAL RESTORE MODE ---
# Set these to your historical range. 
# Example: To restore Nov 4th through Nov 30th:
#START_DATE_STR = "2025-10-01" 
#NEXT_DAY_STR = "2025-12-01"    # Must be 1 day AFTER your target end date!
# =============================================================================

SPLIT_THRESHOLD_SECONDS = 1800
JOIN_GRACE_PERIOD_SECONDS = 10

# =============================================================================
# 2. CLIENT & TABLE CONFIGURATION
# =============================================================================
CLIENTS = [
    {"folder_name": "kesari",   "mongo_prefix": "kesarishop"},
    {"folder_name": "gardenia", "mongo_prefix": "gardenia"},
    {"folder_name": "lanapaws", "mongo_prefix": "lanapaws-client"},
   {"folder_name": "vuvatech", "mongo_prefix": "vuvatech-client"}
]
STANDARD_TABLES = [
    {
        "mongo_suffix": "activity_logs_v2",
        "target_name": "activity_logs_v2",
        "time_col": "time",
        "user_col": "fingerprint",
        "force_mongo_string": ["details"], 
        "complex_cols": []
    },
    {
        "mongo_suffix": "user_sessions_v2",
        "target_name": "user_sessions_v2",
        "time_col": "sessionStart",
        "user_col": "fingerprint",
        "complex_cols": [],
        "is_session_table": True
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
    }
]

COLUMN_RENAMES = {
    "_id": "mongo_id",
    "sessionStart": "session_start",
    "sessionEnd": "session_end_raw",
    "sessionend": "session_end_raw",
    "createdAt": "created_at",
    "userAgent": "user_agent"
}

# =============================================================================
# 3. SPARK INITIALIZATION
# =============================================================================
spark = (
    SparkSession.builder
    .appName("Kesari_Golden_Session_ETL_PROD")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.adaptive.enabled", "true")
    # ✅ CORRECT HADOOP CONFIG: Prevents empty directory markers from generating
    .config("fs.s3a.directory.marker.retention", "keep") 
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
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
                    "$gte": {"$date": f"{START_DATE_STR}T00:00:00.000Z"},
                    "$lt":  {"$date": f"{NEXT_DAY_STR}T00:00:00.000Z"}
                }
            }
        }
    ]
    if force_string_cols:
        conversions = {col_name: {"$toString": f"${col_name}"} for col_name in force_string_cols}
        pipeline.append({"$set": conversions})
    return json.dumps(pipeline)

def targeted_s3_cleanup(base_path, year, month, day):
    """
    Deletes $folder$ markers ONLY in the specified partition.
    """
    try:
        partition_path = f"{base_path}year={year}/month={month}/day={day}/"
        parsed = urlparse(partition_path)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip('/')
        
        s3 = boto3.resource('s3')
        bucket_obj = s3.Bucket(bucket)
        
        markers = bucket_obj.objects.filter(Prefix=prefix)
        deleted_count = 0
        for obj in markers:
            if obj.key.endswith('$folder$'):
                obj.delete()
                deleted_count += 1
        
        if deleted_count > 0:
            log.info(f"🗑️ Removed {deleted_count} markers from partition: {year}/{month}/{day}")
    except Exception as e:
        log.warning(f"⚠️ Targeted cleanup failed: {e}")

def clean_partitions_in_range(base_path, start_date_str, next_day_str):
    """
    🚀 DYNAMIC RANGE CLEANUP: Safely loops through all dates processed by the job
    and cleans them individually. Works for 1-day daily runs OR multi-day restores.
    """
    try:
        start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_dt = datetime.strptime(next_day_str, "%Y-%m-%d")
        
        current_dt = start_dt
        while current_dt < end_dt:
            y = current_dt.strftime("%Y")
            m = current_dt.strftime("%m")
            d = current_dt.strftime("%d")
            
            targeted_s3_cleanup(base_path, y, m, d)
            current_dt += timedelta(days=1)
    except Exception as e:
        log.warning(f"⚠️ Range cleanup loop failed: {e}")

# =============================================================================
# 5. ROBUST PROCESSOR
# =============================================================================
def process_table(client, table_cfg, session_lookup_df=None):
    source = f"{client['mongo_prefix']}_{table_cfg['mongo_suffix']}"
    target = f"{BASE_S3_PATH}{client['folder_name']}/{table_cfg['target_name']}/"

    log.info(f"--> Processing: {source}")

    try:
        pipeline = get_mongo_pipeline(table_cfg["time_col"], table_cfg.get("force_mongo_string", []))
        df = spark.read.format(MONGO_PROVIDER).option("connection.uri", MONGO_URI).option("database", DB).option("collection", source).option("pipeline", pipeline).load()
        if df.rdd.isEmpty(): return None
    except Exception as e:
        log.warning(f"⚠️ Read failed. ({str(e)[:100]}...)"); return None

    if "body" in df.columns: df = df.select("*", "body.*").drop("body")
    for c in table_cfg.get("force_mongo_string", []):
        if c in df.columns: df = df.withColumn(c, to_json(col(c)))
    for c in table_cfg.get("complex_cols", []):
        if c in df.columns: df = df.withColumn(c, to_json(col(c)))
    for old, new in COLUMN_RENAMES.items():
        if old in df.columns: df = df.withColumnRenamed(old, new)

    time_col = COLUMN_RENAMES.get(table_cfg["time_col"], table_cfg["time_col"])
    user_col = table_cfg["user_col"]
    if user_col not in df.columns and "user_id" in df.columns: user_col = "user_id"

    if time_col not in df.columns:
        log.warning(f"!!! SKIPPING {source}: Could not find time column.")
        return None

    df = df.withColumn("event_timestamp", to_timestamp(col(time_col))).withColumn("date", to_date(col("event_timestamp")))
    df = df.filter((col("event_timestamp") >= to_timestamp(lit(f"{START_DATE_STR} 00:00:00"))) & (col("event_timestamp") < to_timestamp(lit(f"{NEXT_DAY_STR} 00:00:00"))))

    if table_cfg.get("is_session_table"):
        log.info("    Applying Session Boundary Rules...")
        df = df.filter(col(user_col).isNotNull())
        
        # 🐞 LOGIC FIX 1: Deterministic Tie-Breaker for Window OrderBy
        sort_cols = [col("event_timestamp")]
        if "mongo_id" in df.columns: sort_cols.append(col("mongo_id"))
        w_user = Window.partitionBy(user_col, "date").orderBy(*sort_cols)

        df = df.withColumn("prev_ts", lag("event_timestamp").over(w_user)).withColumn("is_new_session", when(col("prev_ts").isNull() | ((col("event_timestamp").cast("long") - col("prev_ts").cast("long")) > SPLIT_THRESHOLD_SECONDS), 1).otherwise(0)).withColumn("session_seq", sum_("is_new_session").over(w_user))
        
        w_seq = Window.partitionBy(user_col, "date", "session_seq")
        df = df.withColumn("session_start_marker", min_("event_timestamp").over(w_seq))
        
        if "session_end_raw" in df.columns:
            df = df.withColumn("mongo_end_ts", to_timestamp(col("session_end_raw")))
            df = df.withColumn("session_end", coalesce(max_("mongo_end_ts").over(w_seq), max_("event_timestamp").over(w_seq), col("session_start_marker")))
        else:
            df = df.withColumn("session_end", max_("event_timestamp").over(w_seq))

        df = df.withColumn("session_id", sha2(concat_ws("||", col(user_col), col("session_start_marker").cast("string")), 256))
        df = df.withColumn("session_label", concat(col(user_col), lit("__"), col("session_start_marker").cast("string")))

        drop_cols = ["prev_ts", "is_new_session", "session_seq", "session_start_marker"]
        if "mongo_end_ts" in df.columns: drop_cols.append("mongo_end_ts")
        df = df.withColumn("is_session_assigned", lit(True)).drop(*drop_cols)

    elif session_lookup_df is not None and user_col in df.columns:
        log.info("    Enriching with Session Lookup...")
        joined = df.join(broadcast(session_lookup_df), (df[user_col] == session_lookup_df["sess_user"]) & (df["event_timestamp"] >= session_lookup_df["sess_start_window"]) & (df["event_timestamp"] <= session_lookup_df["sess_end_window"]), "left")
        if "mongo_id" in joined.columns:
            w_best = Window.partitionBy(joined["mongo_id"]).orderBy(abs_(joined["event_timestamp"].cast("long") - joined["sess_start_window"].cast("long")))
            joined = joined.withColumn("rn", row_number().over(w_best)).filter(col("rn") == 1).drop("rn")
        df = joined.withColumn("session_id", coalesce(col("session_id"), concat(col(user_col), lit("_"), col("date").cast("string"), lit("_unassigned"))))
        df = df.withColumn("is_session_assigned", ~col("session_id").endswith("_unassigned")).drop("sess_user", "sess_start_window", "sess_end_window")

    # 🐞 LOGIC FIX 2: Null-Safe Drop Duplicates to prevent Data Skew / OOM
    if "mongo_id" in df.columns:
        df_clean = df.filter(col("mongo_id").isNotNull()).dropDuplicates(["mongo_id"])
        df_nulls = df.filter(col("mongo_id").isNull())
        df = df_clean.unionByName(df_nulls)

    df = df.withColumn("year", year(col("date"))).withColumn("month", format_string("%02d", month(col("date")))).withColumn("day", format_string("%02d", dayofmonth(col("date"))))
    df = df.repartition("year", "month", "day")
    
    log.info(f"    Writing to S3: {target}")
    df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(target)

    # 🎯 TARGETED CLEANUP: Trigger dynamic range cleanup
    clean_partitions_in_range(target, START_DATE_STR, NEXT_DAY_STR)

    return df

# =============================================================================
# 6. MASTER TABLE BUILDER
# =============================================================================
def build_session_master(client, session_df):
    if session_df is None: return
    target = f"{BASE_S3_PATH}{client['folder_name']}/session_master/"
    log.info(f"--> Building Session Master: {target}")
    
    master_df = session_df.groupBy("session_id", "session_label", "fingerprint", "date").agg(min_("event_timestamp").alias("session_start_ts"), max_(to_timestamp(col("session_end"))).alias("session_end_ts"), count_(lit(1)).alias("event_count"))
    master_df = master_df.withColumn("session_duration", round(col("session_end_ts").cast("double") - col("session_start_ts").cast("double"), 4))
    master_df = master_df.withColumn("year", year(col("date"))).withColumn("month", format_string("%02d", month(col("date")))).withColumn("day", format_string("%02d", dayofmonth(col("date"))))
    
    master_df = master_df.dropDuplicates(["session_id"]).repartition("year", "month", "day")
    master_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(target)
    
    # 🎯 TARGETED CLEANUP: Trigger dynamic range cleanup
    clean_partitions_in_range(target, START_DATE_STR, NEXT_DAY_STR)

# =============================================================================
# 7. MAIN ORCHESTRATOR
# =============================================================================
def main():
    log.info("=== KESARI GOLDEN ETL PROD START ===")
    for client in CLIENTS:
        client_name = client["folder_name"]
        log.info(f"\n>>> CLIENT: {client_name} <<<")

        session_cfg = next(t for t in STANDARD_TABLES if t.get("is_session_table"))
        session_df = process_table(client, session_cfg)
        lookup = None
        if session_df is not None:
            session_df.cache()
            build_session_master(client, session_df)
            lookup = session_df.groupBy("session_id").agg(first("fingerprint").alias("sess_user"), (min_("event_timestamp") - expr(f"INTERVAL {JOIN_GRACE_PERIOD_SECONDS} SECONDS")).alias("sess_start_window"), (max_(to_timestamp(col("session_end"))) + expr(f"INTERVAL {JOIN_GRACE_PERIOD_SECONDS} SECONDS")).alias("sess_end_window"))
        
        for table in STANDARD_TABLES:
            if not table.get("is_session_table"): process_table(client, table, lookup)
        
        if session_df is not None: session_df.unpersist()
    log.info("\n=== ETL COMPLETED SUCCESSFULLY ===")
    spark.stop()

if __name__ == "__main__":
    main()