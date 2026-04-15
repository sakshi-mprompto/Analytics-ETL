import os
import sys
import logging
import json
import boto3
import builtins  # Resolves the round() conflict
from urllib.parse import urlparse
from datetime import datetime, timedelta, time, timezone
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StringType, TimestampType, BooleanType, DoubleType, StructType, StructField, LongType, DateType, NullType
from pyspark.sql.functions import (
    col, to_timestamp, to_date, to_json, when, lag,
    sum as sum_, min as min_, max as max_, first,
    count as count_, countDistinct,
    concat_ws, sha2, year, month,
    dayofmonth, format_string, coalesce, lit, concat, round, expr,
    row_number, abs as abs_, broadcast, monotonically_increasing_id,
    from_utc_timestamp, lower, trim
)

# =============================================================================
# 1. INFRASTRUCTURE & SECURE CONFIGURATION
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("golden-mongo-etl-prod")

DRY_RUN = False 

def get_secret(secret_name):
    client = boto3.client("secretsmanager")
    return json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])
    
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://root:mPromptoAdminAi123@172.31.15.148:27017/analytics?authMechanism=SCRAM-SHA-256&authSource=admin"
)

DB = "analytics"
BASE_S3_PATH = "s3a://mongodatamprompt/etl_data/partitioned/"
MONGO_PROVIDER = "com.mongodb.spark.sql.connector.MongoTableProvider"

# =============================================================================
# --- DYNAMIC DATE CONFIGURATION (IST) ---
# =============================================================================
IST = ZoneInfo("Asia/Kolkata")

def ist_date_range_to_utc_bounds(start_date_str: str, next_day_str: str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(next_day_str, "%Y-%m-%d").date()

    start_ist = datetime.combine(start_date, time(0, 0, 0), tzinfo=IST)
    end_ist = datetime.combine(end_date, time(0, 0, 0), tzinfo=IST)

    start_utc = start_ist.astimezone(timezone.utc)
    end_utc = end_ist.astimezone(timezone.utc)

    return (
        start_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        end_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
    )

now_ist = datetime.now(IST)
START_DATE_STR = os.getenv("START_DATE", (now_ist.date() - timedelta(days=1)).strftime("%Y-%m-%d"))
NEXT_DAY_STR   = os.getenv("NEXT_DAY", (now_ist.date()).strftime("%Y-%m-%d"))

START_UTC_ISO, END_UTC_ISO = ist_date_range_to_utc_bounds(START_DATE_STR, NEXT_DAY_STR)
START_UTC_DT = datetime.strptime(START_UTC_ISO, "%Y-%m-%dT%H:%M:%S.000Z").replace(tzinfo=timezone.utc)
END_UTC_DT   = datetime.strptime(END_UTC_ISO,   "%Y-%m-%dT%H:%M:%S.000Z").replace(tzinfo=timezone.utc)

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

log.info(f"📅 Processing IST date: {START_DATE_STR} (UTC window: {START_UTC_ISO} → {END_UTC_ISO})")
log.info(f"🔑 Run ID: {RUN_ID}")
if DRY_RUN: log.info("⚠️ STARTING IN DRY RUN MODE — No data will be written to S3 ⚠️")

SPLIT_THRESHOLD_SECONDS  = 1800
JOIN_GRACE_PERIOD_SECONDS = 60
BROADCAST_SIZE_LIMIT_ROWS = 500_000

# =============================================================================
# 2. CLIENT, TABLE & SCHEMA CONFIGURATION
# =============================================================================
CLIENTS = [
    {"folder_name": "kesari",        "mongo_prefix": "kesarishop"},
   {"folder_name": "gardenia",      "mongo_prefix": "gardenia"},
    {"folder_name": "lanapaws",      "mongo_prefix": "lanapaws-client"},
    {"folder_name": "vuvatech",      "mongo_prefix": "vuvatech-client"},
    {"folder_name": "wellbi",        "mongo_prefix": "wellbi-client"}
]

STANDARD_TABLES = [
    {
        "mongo_suffix": "activity_logs_v2",
        "target_name": "activity_logs_v2",
        "time_col": "createdAt", 
        "user_col": "fingerprint",
        "force_mongo_string": [],               
        "complex_cols": ["userAgent", "user_agent", "details"],  
        "cast_to_date": ["details.timestamp"],
    },
    {
        "mongo_suffix": "user_sessions_v2",
        "target_name": "user_sessions_v2",
        "time_col": "createdAt", 
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
        "complex_cols": ["userAgent", "user_agent"],
    }
]

COLUMN_RENAMES = {
    "_id":          "mongo_id",
    "sessionStart": "session_start",
    "sessionEnd":   "session_end_raw",
    "sessionend":   "session_end_raw",
    "createdAt":    "created_at",
    "userAgent":    "user_agent",
}

SCHEMA_CASTS = {
    "fingerprint":        StringType(),
    "session_id":         StringType(),
    "event_timestamp":    TimestampType(),
    "created_at":         TimestampType(),
    "session_start":      TimestampType(),
    "is_session_assigned": BooleanType(),
    "is_bounce":          BooleanType(),
    "is_engaged":         BooleanType(),  
    "session_duration":   DoubleType(),
}

def enforce_schema(df):
    for col_name, dtype in SCHEMA_CASTS.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(dtype))
    return df

# =============================================================================
# 3. SPARK INITIALIZATION
# =============================================================================
spark = (
    SparkSession.builder
    .appName("Golden_Mongo_ETL_PROD")
    .config("spark.scheduler.mode", "FAIR") 
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.adaptive.enabled", "true")
    .config("fs.s3a.directory.marker.retention", "keep")
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .getOrCreate()
)

# =============================================================================
# 4. AUDIT LOGGING SCHEMA & HELPERS
# =============================================================================
AUDIT_SCHEMA = StructType([
    StructField("run_id",                    StringType(),    True),
    StructField("run_date",                  DateType(),      True),
    StructField("pipeline_run_ts",           TimestampType(), True),
    StructField("client_name",               StringType(),    True),
    StructField("source_table",              StringType(),    True),
    StructField("status",                    StringType(),    True),
    StructField("error_message",             StringType(),    True),
    StructField("stage_failed",              StringType(),    True),
    StructField("is_full_load",              BooleanType(),   True),
    StructField("rows_read_raw",             LongType(),      True),
    StructField("null_user_agent_dropped",   LongType(),      True),
    StructField("bots_dropped",              LongType(),      True),
    StructField("invalid_identity_dropped",  LongType(),      True),
    StructField("mongo_filter_leak_dropped", LongType(),      True),
    StructField("orphans_dropped",           LongType(),      True),
    StructField("final_rows_written",        LongType(),      True),
    StructField("null_timestamp_dropped",    LongType(),      True),
    StructField("duplicate_mongo_id_dropped",LongType(),      True),
])

def make_audit(client, table_cfg):
    return {
        "run_id":                    RUN_ID,
        "run_date":                  datetime.strptime(START_DATE_STR, "%Y-%m-%d").date(),
        "pipeline_run_ts":           datetime.now(timezone.utc).replace(tzinfo=None), 
        "client_name":               client["folder_name"],
        "source_table":              table_cfg["target_name"],
        "status":                    "STARTED",
        "error_message":             None,
        "stage_failed":              None,
        "is_full_load":              table_cfg.get("full_load", False),
        "rows_read_raw":             0,
        "null_user_agent_dropped":   0,
        "bots_dropped":              0,
        "invalid_identity_dropped":  0,
        "mongo_filter_leak_dropped": 0,
        "orphans_dropped":           0,
        "final_rows_written":        0,
        "null_timestamp_dropped":    0,
        "duplicate_mongo_id_dropped":0,
    }

def write_audit_table(all_audit_metrics):
    if not all_audit_metrics or DRY_RUN:
        if DRY_RUN: log.info("🌵 [DRY RUN] Would write ETL audit table.")
        return

    audit_target = f"{BASE_S3_PATH}etl_quality_audit/"
    log.info(f"--> Writing ETL Quality Audit ({len(all_audit_metrics)} rows) to {audit_target}")

    try:
        audit_df = spark.createDataFrame(all_audit_metrics, schema=AUDIT_SCHEMA)
        audit_df = (
            audit_df
            .withColumn("year",  year(to_date(col("run_date"))))
            .withColumn("month", format_string("%02d", month(to_date(col("run_date")))))
            .withColumn("day",   format_string("%02d", dayofmonth(to_date(col("run_date")))))
        )
        audit_df.write.mode("append").partitionBy("year", "month", "day").parquet(audit_target)
        log.info("✅ ETL Quality Audit written successfully.")
    except Exception as e:
        log.error(f"❌ Failed to write ETL Audit Table: {e}", exc_info=True)


# =============================================================================
# 5. HELPER FUNCTIONS
# =============================================================================
def get_mongo_pipeline(time_col, force_string_cols=None, is_full_load=False, skip_mongo_filter=False, cast_to_date=None):
    pipeline = []

    if not is_full_load:
        if not skip_mongo_filter:
            pipeline.append({
                "$match": {
                    time_col: {
                        "$gte": {"$date": START_UTC_ISO},
                        "$lt":  {"$date": END_UTC_ISO}
                    }
                }
            })
        else:
            start_ms = int(START_UTC_DT.timestamp() * 1000)
            end_ms   = int(END_UTC_DT.timestamp()   * 1000)
            pipeline.append({
                "$match": {
                    "$expr": {
                        "$and": [
                            {"$gte": [{"$toLong": f"${time_col}"}, start_ms]},
                            {"$lt":  [{"$toLong": f"${time_col}"}, end_ms]}
                        ]
                    }
                }
            })

    merged_set = {}

    if cast_to_date:
        for col_path in cast_to_date:
            merged_set[col_path] = {
                "$cond": {
                    "if":   {"$eq": [{"$type": f"${col_path}"}, "string"]},
                    "then": {"$toDate": f"${col_path}"},
                    "else": f"${col_path}"
                }
            }

    if force_string_cols:
        for c in force_string_cols:
            merged_set[c] = {"$toString": f"${c}"}

    if merged_set:
        pipeline.append({"$set": merged_set})

    return json.dumps(pipeline)


def targeted_s3_cleanup(base_path, year_s, month_s, day_s):
    if DRY_RUN:
        log.info(f"🌵 [DRY RUN] Would cleanup S3 markers in {year_s}/{month_s}/{day_s}")
        return

    try:
        partition_path = f"{base_path}year={year_s}/month={month_s}/day={day_s}/"
        parsed = urlparse(partition_path)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")

        s3 = boto3.resource("s3")
        bucket_obj = s3.Bucket(bucket)

        deleted_count = 0
        for obj in bucket_obj.objects.filter(Prefix=prefix):
            if obj.key.endswith("$folder$"):
                obj.delete()
                deleted_count += 1

        if deleted_count > 0:
            log.info(f"🗑️ Removed {deleted_count} markers from partition: {year_s}/{month_s}/{day_s}")
    except Exception as e:
        log.warning(f"⚠️ Targeted cleanup failed: {e}")


def clean_partitions_in_range(base_path, start_date_str, next_day_str):
    try:
        start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_dt   = datetime.strptime(next_day_str,   "%Y-%m-%d")

        current_dt = start_dt
        while current_dt < end_dt:
            targeted_s3_cleanup(
                base_path,
                current_dt.strftime("%Y"),
                current_dt.strftime("%m"),
                current_dt.strftime("%d"),
            )
            current_dt += timedelta(days=1)
    except Exception as e:
        log.warning(f"⚠️ Range cleanup loop failed: {e}")

# =============================================================================
# 6. ROBUST PROCESSOR
# =============================================================================
def process_table(client, table_cfg, session_lookup_df=None):
    source       = f"{client['mongo_prefix']}_{table_cfg['mongo_suffix']}"
    is_full_load = table_cfg.get("full_load", False)
    target       = f"{BASE_S3_PATH}{client['folder_name']}/{table_cfg['target_name']}/"

    log.info(f"--> Processing: {source} (Full Load: {is_full_load})")

    audit = make_audit(client, table_cfg)
    cached_df = None 

    # -------------------------------------------------------------------------
    # READ FROM MONGODB & CACHE
    # -------------------------------------------------------------------------
    try:
        pipeline = get_mongo_pipeline(
            table_cfg["time_col"],
            table_cfg.get("force_mongo_string", []),
            is_full_load=is_full_load,
            skip_mongo_filter=table_cfg.get("skip_mongo_filter", False),
            cast_to_date=table_cfg.get("cast_to_date", [])
        )

        df = (
            spark.read.format(MONGO_PROVIDER)
            .option("connection.uri", MONGO_URI)
            .option("database", DB)
            .option("collection", source)
            .option("aggregation.pipeline", pipeline)
            .load()
            .cache() 
        )
        cached_df = df

        if df.limit(1).count() == 0:
            log.warning(f"⚠️ {source} returned 0 rows. Pushing empty shell to S3 to maintain table structure.")
            audit["status"] = "EMPTY"
            
            for req_col in [table_cfg["time_col"], table_cfg["user_col"], "user_agent", "user_id"]:
                if req_col not in df.columns:
                    df = df.withColumn(req_col, lit(None).cast(StringType()))

    except Exception as e:
        log.warning(f"⚠️ Read failed for {source}. ({str(e)[:140]}...)")
        audit["status"] = "FAILED"
        audit["stage_failed"] = "READ"
        audit["error_message"] = str(e)[:500]
        return None, audit

    # -------------------------------------------------------------------------
    # JSON STRINGIFICATION & RENAMES
    # -------------------------------------------------------------------------
    if "body" in df.columns:
        df = df.select("*", "body.*").drop("body")

    for c in table_cfg.get("force_mongo_string", []) + table_cfg.get("complex_cols", []):
        if c in df.columns:
            df = df.withColumn(c, to_json(col(c)))

    for old, new in COLUMN_RENAMES.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    time_col = COLUMN_RENAMES.get(table_cfg["time_col"], table_cfg["time_col"])
    user_col = table_cfg["user_col"]

    if "user_id" in df.columns and "fingerprint" in df.columns:
        valid_user_id = when(
            col("user_id").isNotNull()
            & (lower(trim(col("user_id"))) != "unknown")
            & (lower(trim(col("user_id"))) != "null")
            & (trim(col("user_id")) != ""),
            col("user_id")
        )
        df = df.withColumn("resolved_user_id", coalesce(valid_user_id, col("fingerprint")))
        user_col = "resolved_user_id"
    elif user_col not in df.columns and "user_id" in df.columns:
        user_col = "user_id"

    # =========================================================================
    # 🛑 DATA CLEANSING (SINGLE PASS AGGREGATION)
    # =========================================================================
    try:
        if table_cfg["target_name"] in ["user_sessions_v2", "fingerprint_v2"] and "user_agent" in df.columns:
            bot_pattern = (
                "(?i)("
                "bot|crawler|spider|scraper|headless|"
                "urllib|python-requests|python-urllib|curl|wget|go-http-client|httpclient|"
                "postman|lighthouse|monitoring|"
                "ahrefs|semrush|dotbot|rogerbot|exabot|mj12bot|datanyze|"
                "googlebot|adsbot|bingbot|baiduspider|yandexbot|facebookexternalhit|"
                "gptbot|claudebot|perplexitybot|applebot|ccbot|"
                "mediapartners-google|ia_archiver|petalbot|grobbot|deepseekbot"
                ")"
            )

            stats = df.agg(
                count_("*").alias("total"),
                count_(when(col("user_agent").isNull(), 1)).alias("null_ua"),
                count_(when(col("user_agent").isNotNull() & col("user_agent").rlike(bot_pattern), 1)).alias("bots"),
                count_(when(
                    col("user_agent").isNotNull() &
                    (~col("user_agent").rlike(bot_pattern)) &
                    (col(user_col).isNull() |
                     (lower(trim(col(user_col))) == "unknown") |
                     (lower(trim(col(user_col))) == "null") |
                     (trim(col(user_col)) == "")),
                    1
                )).alias("ghost")
            ).collect()[0]

            audit["rows_read_raw"]            = stats["total"]
            audit["null_user_agent_dropped"]  = stats["null_ua"]
            audit["bots_dropped"]             = stats["bots"]
            audit["invalid_identity_dropped"] = stats["ghost"]

            if stats["null_ua"] > 0:
                log.info(f"⚠️ NULL USER_AGENT: Dropping {stats['null_ua']:,} rows from {source}")
            if stats["bots"] > 0:
                log.info(f"🛡️ BOT FILTER: Dropping {stats['bots']:,} bot rows from {source}")
            if stats["ghost"] > 0:
                log.info(f"👻 IDENTITY FILTER: Dropped {stats['ghost']:,} untrackable rows from {source}")

            df = df.filter(
                col("user_agent").isNotNull() &
                (~col("user_agent").rlike(bot_pattern)) &
                col(user_col).isNotNull() &
                (lower(trim(col(user_col))) != "unknown") &
                (lower(trim(col(user_col))) != "null") &
                (trim(col(user_col)) != "")
            )

        else:
            total = df.count()
            audit["rows_read_raw"] = total
            if user_col in df.columns:
                df = df.filter(
                    col(user_col).isNotNull() &
                    (lower(trim(col(user_col))) != "unknown") &
                    (lower(trim(col(user_col))) != "null") &
                    (trim(col(user_col)) != "")
                )
                dropped = total - df.count()
                audit["invalid_identity_dropped"] = dropped
                if dropped > 0:
                    log.info(f"👻 IDENTITY FILTER: Dropped {dropped:,} untrackable rows from {source}")

    except Exception as e:
        audit["status"] = "FAILED"
        audit["stage_failed"] = "CLEANSING"
        audit["error_message"] = str(e)[:500]
        if cached_df is not None: cached_df.unpersist()
        return None, audit

    # -------------------------------------------------------------------------
    # TIMESTAMP PARSING & IST CONVERSION
    # -------------------------------------------------------------------------
    try:
        if time_col not in df.columns:
            log.warning(f"!!! SKIPPING {source}: time column '{time_col}' not found.")
            audit["status"] = "FAILED"
            audit["stage_failed"] = "TIMESTAMP"
            audit["error_message"] = f"Time column '{time_col}' not found"
            df.unpersist()
            return None, audit

        before_ts = df.count() 
        df = df.withColumn("event_timestamp_utc", to_timestamp(col(time_col)))
        df = df.filter(col("event_timestamp_utc").isNotNull())

        if not is_full_load:
            df = df.filter(
                (col("event_timestamp_utc") >= lit(START_UTC_ISO).cast(TimestampType())) &
                (col("event_timestamp_utc") <  lit(END_UTC_ISO).cast(TimestampType()))
            )
            
        after_ts = df.count() 
        audit["null_timestamp_dropped"] = before_ts - after_ts

        if before_ts - after_ts > 0:
            log.warning(f"⚠️ MONGO FILTER LEAK DETECTED for {source}: Dropped {before_ts - after_ts:,} rows falling outside IST bounds or containing null timestamps.")

        if df.isEmpty():
            log.warning(f"⚠️ {source} has 0 rows after Spark date filter for {START_DATE_STR}. Proceeding to write empty table.")
            audit["status"] = "EMPTY"

        df = df.withColumn("event_timestamp_ist", from_utc_timestamp(col("event_timestamp_utc"), "Asia/Kolkata"))
        df = df.withColumn("date", to_date(col("event_timestamp_ist")))

        for extra_col in ["created_at", "session_start", "session_end_raw"]:
            if extra_col in df.columns:
                df = df.withColumn(f"{extra_col}_utc", to_timestamp(col(extra_col)))
                df = df.withColumn(f"{extra_col}_ist", from_utc_timestamp(col(f"{extra_col}_utc"), "Asia/Kolkata"))

    except Exception as e:
        audit["status"] = "FAILED"
        audit["stage_failed"] = "TIMESTAMP"
        audit["error_message"] = str(e)[:500]
        if cached_df is not None: cached_df.unpersist()
        return None, audit

    # =========================================================================
    # SESSION BOUNDARY LOGIC (user_sessions_v2 only)
    # =========================================================================
    if table_cfg.get("is_session_table"):
        try:
            log.info("    Applying Session Boundary Rules (IST logic)...")
            df = df.filter(col(user_col).isNotNull())

            if not df.isEmpty():
                sort_cols = [col("event_timestamp_ist")]
                if "mongo_id" in df.columns:
                    sort_cols.append(col("mongo_id"))

                w_user = Window.partitionBy(user_col, "date").orderBy(*sort_cols)

                df = (
                    df.withColumn("prev_ts", lag("event_timestamp_ist").over(w_user))
                    .withColumn(
                        "is_new_session",
                        when(
                            col("prev_ts").isNull() |
                            ((col("event_timestamp_ist").cast("long") - col("prev_ts").cast("long")) > SPLIT_THRESHOLD_SECONDS),
                            1
                        ).otherwise(0)
                    )
                    .withColumn("session_seq", sum_("is_new_session").over(w_user))
                )

                w_seq = Window.partitionBy(user_col, "date", "session_seq")
                df = df.withColumn("session_start_marker", min_("event_timestamp_ist").over(w_seq))

                if "session_end_raw_ist" in df.columns:
                    df = df.withColumn(
                        "session_end",
                        when(
                            max_("session_end_raw_ist").over(w_seq) >= max_("event_timestamp_ist").over(w_seq),
                            max_("session_end_raw_ist").over(w_seq)
                        ).otherwise(max_("event_timestamp_ist").over(w_seq))
                    )
                else:
                    df = df.withColumn("session_end", max_("event_timestamp_ist").over(w_seq))

                df = df.withColumn(
                    "session_id",
                    sha2(concat_ws("||", col(user_col), col("session_start_marker").cast("string")), 256)
                )
                df = df.withColumn(
                    "session_label",
                    concat(col(user_col), lit("__"), col("session_start_marker").cast("string"))
                )

                df = (
                    df.withColumn("is_session_assigned", lit(True))
                    .drop("prev_ts", "is_new_session", "session_seq", "session_start_marker")
                )
            else:
                df = df.withColumn("session_id", lit(None).cast(StringType())) \
                       .withColumn("session_label", lit(None).cast(StringType())) \
                       .withColumn("is_session_assigned", lit(False)) \
                       .withColumn("session_end", lit(None).cast(TimestampType()))

        except Exception as e:
            audit["status"] = "FAILED"
            audit["stage_failed"] = "SESSION_BOUNDARY"
            audit["error_message"] = str(e)[:500]
            df.unpersist()
            return None, audit

    # =========================================================================
    # SESSION LOOKUP ENRICHMENT (all non-session tables)
    # =========================================================================
    elif session_lookup_df is not None and user_col in df.columns and not is_full_load:
        try:
            log.info("    Enriching with Session Lookup (IST logic)...")
            
            if not df.isEmpty():
                joined = df.join(
                    session_lookup_df,
                    (df[user_col] == session_lookup_df["sess_user"]) &
                    (df["event_timestamp_ist"] >= session_lookup_df["sess_start_window"]) &
                    (df["event_timestamp_ist"] <= session_lookup_df["sess_end_window"]),
                    "left"
                )

                if "mongo_id" in joined.columns:
                    best_key = "mongo_id"
                else:
                    joined = joined.withColumn("_row_id", monotonically_increasing_id())
                    best_key = "_row_id"

                w_best = Window.partitionBy(joined[best_key]).orderBy(
                    abs_(joined["event_timestamp_ist"].cast("long") - joined["sess_start_window"].cast("long"))
                )
                joined = joined.withColumn("rn", row_number().over(w_best)).filter(col("rn") == 1).drop("rn")

                df = joined.withColumn(
                    "session_id",
                    coalesce(
                        col("session_id"),
                        concat(col(user_col), lit("_"), col("date").cast("string"), lit("_unassigned"))
                    )
                )
                df = (
                    df.withColumn("is_session_assigned", ~col("session_id").endswith("_unassigned"))
                    .drop("sess_user", "sess_start_window", "sess_end_window")
                )
                if "_row_id" in df.columns:
                    df = df.drop("_row_id")
            else:
                df = df.withColumn("session_id", lit(None).cast(StringType())) \
                       .withColumn("is_session_assigned", lit(False))

        except Exception as e:
            audit["status"] = "FAILED"
            audit["stage_failed"] = "SESSION_ENRICHMENT"
            audit["error_message"] = str(e)[:500]
            df.unpersist()
            return None, audit


    # -------------------------------------------------------------------------
    # CASCADING CLEANUP — DROP ORPHANS (BOTS)
    # -------------------------------------------------------------------------
    CHILD_TABLES_REQUIRING_SESSION = {
        "activity_logs_v2", 
        "nudge_responses_v2", 
        "nudge_recommendations_v2"
    }
    
    if (
        table_cfg["target_name"] in CHILD_TABLES_REQUIRING_SESSION
        and "is_session_assigned" in df.columns
    ):
        if not df.isEmpty():
            before_clean = df.count()
            df = df.filter(col("is_session_assigned") == True)
            orphans_dropped = before_clean - df.count()
            audit["orphans_dropped"] = orphans_dropped
            
            if orphans_dropped > 0:
                log.info(f"🧹 CASCADING CLEANUP: Dropped {orphans_dropped:,} orphaned bot records from {source}")

    # -------------------------------------------------------------------------
    # DEDUPLICATION on mongo_id
    # -------------------------------------------------------------------------
    if "mongo_id" in df.columns and not df.isEmpty():
        df_clean = df.filter(col("mongo_id").isNotNull()).dropDuplicates(["mongo_id"])
        df_nulls = df.filter(col("mongo_id").isNull())
        df = df_clean.unionByName(df_nulls)

    # -------------------------------------------------------------------------
    # FINAL COLUMN CLEANUP
    # -------------------------------------------------------------------------
    if "event_timestamp_ist" in df.columns:
        if "event_timestamp" in df.columns:
            df = df.drop("event_timestamp")
        if "event_timestamp_utc" in df.columns:
            df = df.drop("event_timestamp_utc")
        df = df.withColumnRenamed("event_timestamp_ist", "event_timestamp")

    for target_col in ["created_at", "session_start", "session_end_raw"]:
        ist_col = f"{target_col}_ist"
        utc_col = f"{target_col}_utc"
        if ist_col in df.columns:
            if target_col in df.columns:
                df = df.drop(target_col)
            if utc_col in df.columns:
                df = df.drop(utc_col)
            df = df.withColumnRenamed(ist_col, target_col)

    df = enforce_schema(df)

    # =========================================================================
    # ✨ NEW: CATCH AND CAST VOID (NullType) COLUMNS FOR PARQUET COMPATIBILITY
    # =========================================================================
    for field in df.schema.fields:
        if isinstance(field.dataType, NullType):
            log.info(f"🔄 TYPE FIX: Casting VOID column '{field.name}' to StringType for Parquet compatibility.")
            df = df.withColumn(field.name, col(field.name).cast(StringType()))

    # -------------------------------------------------------------------------
    # ADD PARTITION COLUMNS & WRITE
    # -------------------------------------------------------------------------
    if not df.isEmpty():
        df = (
            df.withColumn("year",  year(col("date")))
            .withColumn("month", format_string("%02d", month(col("date"))))
            .withColumn("day",   format_string("%02d", dayofmonth(col("date"))))
        )
    else:
        dummy_date = datetime.strptime(START_DATE_STR, "%Y-%m-%d")
        df = (
            df.withColumn("year", lit(dummy_date.year))
            .withColumn("month", lit(f"{dummy_date.month:02d}"))
            .withColumn("day", lit(f"{dummy_date.day:02d}"))
        )

    df = df.coalesce(4)
    row_count = df.count() 

    if DRY_RUN:
        log.info(f"🌵 [DRY RUN] Would write {row_count:,} rows to {target}")
        if audit.get("status") != "EMPTY":
            audit["status"] = "SUCCESS"
        audit["final_rows_written"] = row_count
    else:
        try:
            df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(target)
            log.info(f"✅ Written {row_count:,} rows to {target}")
            if not is_full_load:
                clean_partitions_in_range(target, START_DATE_STR, NEXT_DAY_STR)
            if audit.get("status") != "EMPTY":
                audit["status"] = "SUCCESS"
            audit["final_rows_written"] = row_count
        except Exception as e:
            log.error(f"❌ Parquet write failed for {target}: {e}", exc_info=True)
            audit["status"] = "FAILED"
            audit["stage_failed"] = "WRITE"
            audit["error_message"] = str(e)[:500]
            df.unpersist()
            return None, audit

    if cached_df is not None: cached_df.unpersist()
    return df, audit

# =============================================================================
# 6. SESSION MASTER BUILDER
# =============================================================================
def build_session_master(client, session_df, activity_df=None):
    if session_df is None:
        return

    target = f"{BASE_S3_PATH}{client['folder_name']}/session_master/"
    log.info(f"--> Building Session Master: {target}")

    sess_user_col = (
        "fingerprint" if "fingerprint" in session_df.columns
        else "user_id" if "user_id" in session_df.columns
        else None
    )
    if not sess_user_col:
        raise RuntimeError("Session DF has neither fingerprint nor user_id column.")

    master_df = session_df.groupBy("session_id", "session_label", sess_user_col, "date").agg(
        min_("event_timestamp").alias("session_start_ts"),
        max_("session_end").alias("session_end_ts"),
        count_(lit(1)).alias("event_count")
    )

    master_df = master_df.withColumn(
        "session_duration",
        round(col("session_end_ts").cast("double") - col("session_start_ts").cast("double"), 4)
    )

    if activity_df is not None and "session_id" in activity_df.columns:
        log.info("    Joining activity_logs to session_master for scroll/click/page counts...")

        activity_df = activity_df.repartition(200, "session_id")

        if "activity" not in activity_df.columns:
            log.warning("    'activity' column missing from activity_logs. Injecting nulls.")
            activity_df = activity_df.withColumn("activity", lit(None).cast(StringType()))

        if "page" not in activity_df.columns:
            log.warning("    'page' column missing from activity_logs. Injecting nulls.")
            activity_df = activity_df.withColumn("page", lit(None).cast(StringType()))

        activity_agg = activity_df.groupBy("session_id").agg(
            sum_(when(col("activity") == "scroll", 1).otherwise(0)).alias("scroll_count"),
            sum_(when(col("activity") == "click",  1).otherwise(0)).alias("click_count"),
            countDistinct(when(col("page").isNotNull(), col("page"))).alias("unique_pages")
        )

        master_df = master_df.join(activity_agg, on="session_id", how="left")
        master_df = master_df.fillna({"scroll_count": 0, "click_count": 0, "unique_pages": 0})

    else:
        log.warning("    ⚠️ activity_df not available — scroll_count/click_count/unique_pages defaulting to 0")
        master_df = (
            master_df
            .withColumn("scroll_count", lit(0))
            .withColumn("click_count",  lit(0))
            .withColumn("unique_pages", lit(0))
        )

    is_bounce = (
        (col("session_duration") < 10.0) &
        (col("scroll_count") == 0) &
        (col("click_count") == 0) &
        (col("unique_pages") <= 1)
    )

    is_engaged = (
        ~is_bounce &
        (col("session_duration") >= 10.0) &
        (
            (col("scroll_count") > 0) |
            (col("click_count") > 0) |
            (col("unique_pages") > 1)
        )
    )

    master_df = (
        master_df
        .withColumn("is_bounce",  is_bounce)
        .withColumn("is_engaged", is_engaged)
    )

    master_df = enforce_schema(master_df)

    if not master_df.isEmpty():
        master_df = (
            master_df
            .withColumn("year",  year(col("date")))
            .withColumn("month", format_string("%02d", month(col("date"))))
            .withColumn("day",   format_string("%02d", dayofmonth(col("date"))))
        )
    else:
        dummy_date = datetime.strptime(START_DATE_STR, "%Y-%m-%d")
        master_df = (
            master_df.withColumn("year", lit(dummy_date.year))
            .withColumn("month", lit(f"{dummy_date.month:02d}"))
            .withColumn("day", lit(f"{dummy_date.day:02d}"))
        )

    master_df = master_df.coalesce(4)
    master_count = master_df.count()

    if DRY_RUN:
        log.info(f"🌵 [DRY RUN] Would write {master_count:,} master rows to {target}")
    else:
        try:
            master_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(target)
            log.info(f"✅ Written {master_count:,} master rows to {target}")
            clean_partitions_in_range(target, START_DATE_STR, NEXT_DAY_STR)
        except Exception as e:
            log.error(f"❌ Parquet write failed for session master {target}: {e}", exc_info=True)
            raise

# =============================================================================
# 8. MAIN ORCHESTRATOR
# =============================================================================
def main():
    log.info("=== GOLDEN MONGO ETL PROD START ===")
    all_audit_metrics = []

    def process_client(client):
        client_audits = []
        client_name = client["folder_name"]
        log.info(f"\n>>> CLIENT: {client_name} <<<")

        session_cfg = next(t for t in STANDARD_TABLES if t.get("is_session_table"))

        try:
            session_df, sess_audit = process_table(client, session_cfg)
            client_audits.append(sess_audit)
        except Exception as e:
            log.error(f"❌ Base session table failed for {client_name}: {e}", exc_info=True)
            session_df = None

        lookup          = None
        activity_logs_df = None

        if session_df is not None and not session_df.isEmpty():
            session_df.cache()

            try:
                w_lookup = Window.partitionBy("session_id").orderBy("event_timestamp")
                sess_identity_col = "resolved_user_id" if "resolved_user_id" in session_df.columns else "fingerprint"
                
                lookup_df = session_df.withColumn(
                    "sess_user",
                    first(sess_identity_col, ignorenulls=True).over(w_lookup)
                ).groupBy("session_id", "sess_user").agg(
                    (min_("event_timestamp") - expr(f"INTERVAL {JOIN_GRACE_PERIOD_SECONDS} SECONDS")).alias("sess_start_window"),
                    (max_("session_end")      + expr(f"INTERVAL {JOIN_GRACE_PERIOD_SECONDS} SECONDS")).alias("sess_end_window")
                )

                lookup = broadcast(lookup_df)
                log.info(f"✅ Session lookup broadcast for {client_name}")
            except Exception as e:
                log.warning(f"⚠️ Lookup build failed for {client_name}, proceeding without session enrichment: {e}")
                lookup = None

            activity_cfg = next((t for t in STANDARD_TABLES if t["target_name"] == "activity_logs_v2"), None)
            if activity_cfg:
                try:
                    activity_logs_df, act_audit = process_table(client, activity_cfg, lookup)
                    client_audits.append(act_audit)
                    if activity_logs_df is not None:
                        activity_logs_df.cache()
                        log.info(f"✅ activity_logs_v2 cached for session master join")
                except Exception as e:
                    log.warning(f"⚠️ Could not load activity_logs for {client_name}: {e}")
                    activity_logs_df = None

            try:
                build_session_master(client, session_df, activity_df=activity_logs_df)
            except Exception as e:
                log.error(f"❌ Session master failed for {client_name}: {e}", exc_info=True)

        for table in STANDARD_TABLES:
            if table.get("is_session_table"):
                continue
            if table["target_name"] == "activity_logs_v2" and session_df is not None and not session_df.isEmpty():
                log.info(f"    Skipping activity_logs_v2 — already processed above")
                continue
            try:
                _, tbl_audit = process_table(client, table, lookup)
                client_audits.append(tbl_audit)
            except Exception as e:
                log.error(f"❌ Table {table['target_name']} failed for {client_name}: {e}", exc_info=True)

        if session_df is not None:
            session_df.unpersist()
        if activity_logs_df is not None:
            activity_logs_df.unpersist()

        log.info(f"✅ CLIENT {client_name} complete — {len(client_audits)} tables processed")
        return client_audits

    MAX_PARALLEL_CLIENTS = min(3, len(CLIENTS))
    log.info(f"🚀 Processing {len(CLIENTS)} clients with {MAX_PARALLEL_CLIENTS} parallel workers")

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_CLIENTS) as executor:
        futures = {executor.submit(process_client, client): client["folder_name"] for client in CLIENTS}
        for future in as_completed(futures):
            client_name = futures[future]
            try:
                client_audits = future.result()
                all_audit_metrics.extend(client_audits)
            except Exception as e:
                log.error(f"❌ Pipeline failed for client {client_name}: {e}", exc_info=True)

    write_audit_table(all_audit_metrics)

    log.info("\n=== ETL COMPLETED SUCCESSFULLY ===")
    spark.stop()

if __name__ == "__main__":
    test_client = None
    for arg in sys.argv:
        if arg.lower() in [c["folder_name"].lower() for c in CLIENTS]:
            test_client = arg.lower()
            break

    if test_client:
        log.info(f"🛠️  ISOLATION MODE: Running ONLY for client '{test_client}'")
        CLIENTS = [c for c in CLIENTS if c["folder_name"].lower() == test_client]

    main()