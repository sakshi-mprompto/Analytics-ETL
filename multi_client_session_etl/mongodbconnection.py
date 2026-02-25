import os
import logging
import json
import boto3
from urllib.parse import urlparse
from datetime import datetime, timedelta, time, timezone
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StringType, TimestampType, BooleanType, DoubleType
from pyspark.sql.functions import (
    col, to_timestamp, to_date, to_json, when, lag,
    sum as sum_, min as min_, max as max_, first,
    count as count_, concat_ws, sha2, year, month,
    dayofmonth, format_string, coalesce, lit, concat, round, expr,
    row_number, abs as abs_, broadcast, monotonically_increasing_id,
    from_utc_timestamp, lower
)

# =============================================================================
# 1. INFRASTRUCTURE & SECURE CONFIGURATION
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("golden-mongo-etl-prod")

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

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
    end_date   = datetime.strptime(next_day_str, "%Y-%m-%d").date()

    start_ist = datetime.combine(start_date, time(0, 0, 0), tzinfo=IST)
    end_ist   = datetime.combine(end_date,   time(0, 0, 0), tzinfo=IST)

    start_utc = start_ist.astimezone(timezone.utc)
    end_utc   = end_ist.astimezone(timezone.utc)

    return start_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z"), end_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")

now_ist = datetime.now(IST)
START_DATE_STR = (now_ist.date() - timedelta(days=1)).strftime("%Y-%m-%d")
NEXT_DAY_STR   = (now_ist.date()).strftime("%Y-%m-%d")

# restore mode
# START_DATE_STR = "2025-11-01"
# NEXT_DAY_STR = "2025-12-01"

START_UTC_ISO, END_UTC_ISO = ist_date_range_to_utc_bounds(START_DATE_STR, NEXT_DAY_STR)

log.info(f"📅 Processing IST date: {START_DATE_STR} (UTC window: {START_UTC_ISO} → {END_UTC_ISO})")

SPLIT_THRESHOLD_SECONDS = 1800
JOIN_GRACE_PERIOD_SECONDS = 60
BROADCAST_SIZE_LIMIT_ROWS = 500_000

# =============================================================================
# 2. BOT FILTERING (UA-based fingerprint exclusion)
# =============================================================================
# Rule: If a fingerprint ever has a bot-like UA (in fingerprint_v2), remove ALL data for that fingerprint.
BOT_PATTERNS = [
    "bot", "spider", "crawler", "headless", "headlesschrome",
    "selenium", "puppeteer", "playwright", "phantom",
    "python-requests", "scrapy", "aiohttp", "curl", "wget", "axios",
    "go-http-client", "java/"
]

def bot_ua_condition(ua_col_name: str):
    """
    Returns a Spark Column (boolean) that is True when UA looks like a bot.
    """
    ua = lower(coalesce(col(ua_col_name), lit("")))
    cond = None
    for p in BOT_PATTERNS:
        c = ua.contains(p)
        cond = c if cond is None else (cond | c)
    return cond if cond is not None else lit(False)

# =============================================================================
# 3. CLIENT, TABLE & SCHEMA CONFIGURATION
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
        "complex_cols": ["userAgent", "user_agent"]
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
        "complex_cols": ["userAgent", "user_agent"],
        # IMPORTANT: make fingerprint FULL LOAD for stable bot detection
        "full_load": True
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

SCHEMA_CASTS = {
    "fingerprint": StringType(),
    "session_id": StringType(),
    "event_timestamp": TimestampType(),
    "created_at": TimestampType(),
    "session_start": TimestampType(),
    "is_session_assigned": BooleanType(),
    "is_bounce": BooleanType(),
    "session_duration": DoubleType()
}

def enforce_schema(df):
    for col_name, dtype in SCHEMA_CASTS.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(dtype))
    return df

# =============================================================================
# 4. SPARK INITIALIZATION
# =============================================================================
spark = (
    SparkSession.builder
    .appName("Golden_Mongo_ETL_PROD")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.adaptive.enabled", "true")
    .config("fs.s3a.directory.marker.retention", "keep")
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .getOrCreate()
)

# =============================================================================
# 5. HELPER FUNCTIONS
# =============================================================================
def get_mongo_pipeline(time_col, force_string_cols=None, is_full_load=False):
    pipeline = []
    if not is_full_load:
        pipeline.append({
            "$match": {
                time_col: {
                    "$gte": {"$date": START_UTC_ISO},
                    "$lt":  {"$date": END_UTC_ISO}
                }
            }
        })
    if force_string_cols:
        conversions = {col_name: {"$toString": f"${col_name}"} for col_name in force_string_cols}
        pipeline.append({"$set": conversions})
    return json.dumps(pipeline)

def targeted_s3_cleanup(base_path, year, month, day):
    if DRY_RUN:
        log.info(f"🌵 [DRY RUN] Would cleanup S3 markers in {year}/{month}/{day}")
        return

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
# 6. ROBUST PROCESSOR
# =============================================================================
def process_table(client, table_cfg, session_lookup_df=None, bot_fp_df=None):
    """
    bot_fp_df: DataFrame with single column 'fingerprint' containing bot fingerprints to remove globally.
    """
    source = f"{client['mongo_prefix']}_{table_cfg['mongo_suffix']}"
    is_full_load = table_cfg.get("full_load", False)
    target = f"{BASE_S3_PATH}{client['folder_name']}/{table_cfg['target_name']}/"

    log.info(f"--> Processing: {source} (Full Load: {is_full_load})")

    try:
        pipeline = get_mongo_pipeline(
            table_cfg["time_col"],
            table_cfg.get("force_mongo_string", []),
            is_full_load=is_full_load
        )

        df = (
            spark.read.format(MONGO_PROVIDER)
            .option("connection.uri", MONGO_URI)
            .option("database", DB)
            .option("collection", source)
            # IMPORTANT FIX for Mongo Spark connector v10.x
            .option("aggregation.pipeline", pipeline)
            .load()
        )

        if df.isEmpty():
            log.warning(f"⚠️ {source} returned 0 rows. Check Mongo field type for {table_cfg['time_col']}.")
            return None

    except Exception as e:
        log.warning(f"⚠️ Read failed for {source}. ({str(e)[:140]}...)")
        return None

    if "body" in df.columns:
        df = df.select("*", "body.*").drop("body")

    # Force "details" or other fields to be JSON strings (if requested)
    for c in table_cfg.get("force_mongo_string", []):
        if c in df.columns:
            df = df.withColumn(c, to_json(col(c)))

    # Convert complex columns to JSON strings (if requested)
    for c in table_cfg.get("complex_cols", []):
        if c in df.columns:
            df = df.withColumn(c, to_json(col(c)))

    # Column renames
    for old, new in COLUMN_RENAMES.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    time_col = COLUMN_RENAMES.get(table_cfg["time_col"], table_cfg["time_col"])
    user_col = table_cfg["user_col"]
    if user_col not in df.columns and "user_id" in df.columns:
        user_col = "user_id"

    if time_col not in df.columns:
        log.warning(f"!!! SKIPPING {source}: Could not find time column '{time_col}'.")
        return None

    # --- TIMESTAMP CREATION & HARD UTC FILTER (for incremental loads) ---
    df = df.withColumn("event_timestamp_utc", to_timestamp(col(time_col)))
    df = df.filter(col("event_timestamp_utc").isNotNull())

    if not is_full_load:
        # Hard boundary to protect against Mongo $match type mismatch
        before_count = df.count()
        df = df.filter(
            (col("event_timestamp_utc") >= lit(START_UTC_ISO).cast(TimestampType())) &
            (col("event_timestamp_utc") <  lit(END_UTC_ISO).cast(TimestampType()))
        )
        after_count = df.count()
        if before_count != after_count:
            log.warning(
                f"⚠️ FILTER LEAK CHECK for {source}: Mongo returned {before_count:,} rows, "
                f"{after_count:,} rows in UTC window {START_UTC_ISO} → {END_UTC_ISO}. "
                f"If this happens often, Mongo date field type may be string not BSON Date."
            )
        if df.isEmpty():
            log.warning(f"⚠️ {source} has 0 rows after Spark date filter for {START_DATE_STR}.")
            return None

    # --- BOT REMOVAL (GLOBAL) ---
    if bot_fp_df is not None and user_col in df.columns and table_cfg.get("target_name") != "fingerprint_v2":
        # IMPORTANT: don't use bot_fp_df to filter fingerprint_v2 itself
        before = df.count()
        df = df.join(bot_fp_df, on=user_col, how="left_anti")
        after = df.count()
        if before != after:
            log.info(f"🤖 Removed {before - after:,} bot rows from {source} using key '{user_col}'.")
        if df.isEmpty():
            log.warning(f"⚠️ {source} became empty after bot removal.")
            return None

    # --- IST conversion + date partitioning base ---
    df = df.withColumn("event_timestamp_ist", from_utc_timestamp(col("event_timestamp_utc"), "Asia/Kolkata"))
    df = df.withColumn("date", to_date(col("event_timestamp_ist")))

    # Extra columns IST conversions
    for extra_col in ["created_at", "session_start", "session_end_raw"]:
        if extra_col in df.columns:
            df = df.withColumn(f"{extra_col}_utc", to_timestamp(col(extra_col)))
            df = df.withColumn(f"{extra_col}_ist", from_utc_timestamp(col(f"{extra_col}_utc"), "Asia/Kolkata"))

    # --- Sessionization for session table ---
    if table_cfg.get("is_session_table"):
        log.info("    Applying Session Boundary Rules (IST logic)...")
        df = df.filter(col(user_col).isNotNull())

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
        df = df.withColumn("session_label", concat(col(user_col), lit("__"), col("session_start_marker").cast("string")))

        drop_cols = ["prev_ts", "is_new_session", "session_seq", "session_start_marker"]
        df = df.withColumn("is_session_assigned", lit(True)).drop(*drop_cols)

    # --- Enrichment joins for non-session tables ---
    elif session_lookup_df is not None and user_col in df.columns and not is_full_load:
        log.info("    Enriching with Session Lookup (IST logic)...")
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
            coalesce(col("session_id"), concat(col(user_col), lit("_"), col("date").cast("string"), lit("_unassigned")))
        )
        df = (
            df.withColumn("is_session_assigned", ~col("session_id").endswith("_unassigned"))
              .drop("sess_user", "sess_start_window", "sess_end_window")
        )
        if "_row_id" in df.columns:
            df = df.drop("_row_id")

    # Deduplicate by mongo_id when present
    if "mongo_id" in df.columns:
        df_clean = df.filter(col("mongo_id").isNotNull()).dropDuplicates(["mongo_id"])
        df_nulls = df.filter(col("mongo_id").isNull())
        df = df_clean.unionByName(df_nulls)

    # Replace timestamps with IST version
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

    # Partition columns
    df = (
        df.withColumn("year", year(col("date")))
          .withColumn("month", format_string("%02d", month(col("date"))))
          .withColumn("day", format_string("%02d", dayofmonth(col("date"))))
    )

    df = df.repartition("year", "month", "day")
    row_count = df.count()

    if DRY_RUN:
        log.info(f"🌵 [DRY RUN] Would write {row_count:,} rows to {target}")
    else:
        try:
            df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(target)
            log.info(f"✅ Written {row_count:,} rows to {target}")

            if not is_full_load:
                clean_partitions_in_range(target, START_DATE_STR, NEXT_DAY_STR)
        except Exception as e:
            log.error(f"❌ Parquet write failed for {target}: {e}", exc_info=True)
            raise

    return df

# =============================================================================
# 7. MASTER TABLE BUILDER
# =============================================================================
def build_session_master(client, session_df):
    if session_df is None:
        return

    target = f"{BASE_S3_PATH}{client['folder_name']}/session_master/"
    log.info(f"--> Building Session Master: {target}")

    sess_user_col = "fingerprint" if "fingerprint" in session_df.columns else ("user_id" if "user_id" in session_df.columns else None)
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
    master_df = master_df.withColumn("is_bounce", col("event_count") == 1)

    master_df = enforce_schema(master_df)

    master_df = (
        master_df.withColumn("year", year(col("date")))
                 .withColumn("month", format_string("%02d", month(col("date"))))
                 .withColumn("day", format_string("%02d", dayofmonth(col("date"))))
    )

    master_df = master_df.dropDuplicates(["session_id"]).repartition("year", "month", "day")
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
    if DRY_RUN:
        log.info("⚠️ STARTING IN DRY RUN MODE - No data will be written to S3 ⚠️")

    log.info("=== GOLDEN MONGO ETL PROD START ===")
    for client in CLIENTS:
        try:
            client_name = client["folder_name"]
            log.info(f"\n>>> CLIENT: {client_name} <<<")

            # --- 1) Build bot fingerprint list from fingerprint_v2 (FULL LOAD) ---
            fp_cfg = next(t for t in STANDARD_TABLES if t["target_name"] == "fingerprint_v2")
            fp_df = process_table(client, fp_cfg, session_lookup_df=None, bot_fp_df=None)

            bot_fp_df = None
            if fp_df is not None:
                ua_col = None
                if "user_agent" in fp_df.columns:
                    ua_col = "user_agent"
                elif "userAgent" in fp_df.columns:
                    ua_col = "userAgent"

                if ua_col:
                    bot_fp_df = fp_df.filter(bot_ua_condition(ua_col)).select("fingerprint").distinct()
                    bot_count = bot_fp_df.count()
                    bot_fp_df = broadcast(bot_fp_df)
                    log.info(f"[{client_name}] Bot fingerprints detected (UA-only): {bot_count:,}")
                else:
                    log.warning(f"[{client_name}] fingerprint_v2 has no UA column; bot filtering DISABLED.")
            else:
                log.warning(f"[{client_name}] fingerprint_v2 could not be read; bot filtering DISABLED.")

            # --- 2) Process session table (bot-filtered) ---
            session_cfg = next(t for t in STANDARD_TABLES if t.get("is_session_table"))
            try:
                session_df = process_table(client, session_cfg, session_lookup_df=None, bot_fp_df=bot_fp_df)
            except Exception as e:
                log.error(f"❌ Base session table failed for {client_name}: {e}", exc_info=True)
                session_df = None

            # --- 3) Build session master + lookup for enrichment ---
            lookup = None
            if session_df is not None:
                session_df.cache()

                try:
                    build_session_master(client, session_df)
                except Exception as e:
                    log.error(f"❌ Session master failed for {client_name}: {e}", exc_info=True)

                try:
                    w_lookup = Window.partitionBy("session_id").orderBy("event_timestamp")

                    lookup_df = session_df.withColumn(
                        "sess_user",
                        first("fingerprint", ignorenulls=True).over(w_lookup)
                    ).groupBy("session_id", "sess_user").agg(
                        (min_("event_timestamp") - expr(f"INTERVAL {JOIN_GRACE_PERIOD_SECONDS} SECONDS")).alias("sess_start_window"),
                        (max_("session_end") + expr(f"INTERVAL {JOIN_GRACE_PERIOD_SECONDS} SECONDS")).alias("sess_end_window")
                    )

                    lookup_count = lookup_df.count()
                    if lookup_count < BROADCAST_SIZE_LIMIT_ROWS:
                        lookup = broadcast(lookup_df)
                        log.info(f"Using broadcast join ({lookup_count:,} rows) for {client_name}")
                    else:
                        lookup = lookup_df
                        log.warning(f"Skipping broadcast — {lookup_count:,} rows exceeds limit for {client_name}")
                except Exception as e:
                    log.warning(f"⚠️ Lookup build failed for {client_name}, proceeding without session enrichment: {e}")
                    lookup = None

            # --- 4) Process remaining tables (skip fingerprint_v2 & session table), bot-filtered ---
            for table in STANDARD_TABLES:
                if table.get("is_session_table"):
                    continue
                if table["target_name"] == "fingerprint_v2":
                    continue  # already processed above
                try:
                    process_table(client, table, session_lookup_df=lookup, bot_fp_df=bot_fp_df)
                except Exception as e:
                    log.error(f"❌ Table {table['target_name']} failed for {client_name}: {e}", exc_info=True)

            if session_df is not None:
                session_df.unpersist()

        except Exception as e:
            log.error(f"❌ Pipeline failed for client {client['folder_name']}: {e}", exc_info=True)
            continue

    log.info("\n=== ETL COMPLETED SUCCESSFULLY ===")
    spark.stop()

if __name__ == "__main__":
    main()