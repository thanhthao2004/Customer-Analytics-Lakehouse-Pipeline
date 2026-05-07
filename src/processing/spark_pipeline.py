"""
PySpark processing pipeline — Cloud S3 / Cloudflare R2 edition.

Environment variables (set as GitHub Secrets):
  S3_BUCKET, S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
  POSTGRES_HOST/PORT/USER/PASSWORD/DB, GEMINI_API_KEY, SPARK_DRIVER_MEMORY
  SPARK_MODE: 'incremental' (default) | 'full'
    incremental — read only today's UTC parquet files (daily run)
    full        — read all parquet files (backfill / first run)
"""

import os
import sys
import time
from functools import reduce
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from loguru import logger

# ---------------------------------------------------------------------------
# Config from environment — no local file fallback in cloud mode
# ---------------------------------------------------------------------------
S3_BUCKET    = os.environ["S3_BUCKET"]
S3_ENDPOINT  = os.environ.get("S3_ENDPOINT_URL", "")
AWS_KEY      = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET   = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION   = os.environ.get("AWS_REGION", "auto")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "analytics")
POSTGRES_PASS = os.environ.get("POSTGRES_PASSWORD", "")
POSTGRES_DB   = os.environ.get("POSTGRES_DB", "customer_analytics")

GEMINI_API_KEY  = os.environ.get("GEMINI_API_KEY", "")
DRIVER_MEMORY   = os.environ.get("SPARK_DRIVER_MEMORY", "4g")
EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "2g")
SPARK_MODE      = os.environ.get("SPARK_MODE", "incremental")  # incremental | full

JDBC_URL    = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_DRIVER = "org.postgresql.Driver"

# S3 paths — incremental reads today's UTC partition; full reads everything
TODAY_UTC = datetime.utcnow().strftime("%Y%m%d")
if SPARK_MODE == "incremental":
    RAW_SHOPEE_PATH = f"s3a://{S3_BUCKET}/raw/shopee/{TODAY_UTC}/*.parquet"
    RAW_LAZADA_PATH = f"s3a://{S3_BUCKET}/raw/lazada/{TODAY_UTC}/*.parquet"
else:
    RAW_SHOPEE_PATH = f"s3a://{S3_BUCKET}/raw/shopee/*/*.parquet"
    RAW_LAZADA_PATH = f"s3a://{S3_BUCKET}/raw/lazada/*/*.parquet"

DELTA_OUT_PATH  = f"s3a://{S3_BUCKET}/processed_delta"
QUARANTINE_PATH = f"s3a://{S3_BUCKET}/quarantine_reviews"

# JDBC write mode: append for incremental (accumulate), overwrite for full backfill
JDBC_WRITE_MODE = "append" if SPARK_MODE == "incremental" else "overwrite"

# Gemini settings
GEMINI_MODEL        = "gemini-2.0-flash"
GEMINI_BATCH_SIZE   = 50
GEMINI_MIN_INTERVAL = 4.1   # ~14 req/min vs free tier 15


# ---------------------------------------------------------------------------
# Gemini sentiment
# ---------------------------------------------------------------------------
def _label_batch_via_gemini(comments: list) -> list:
    from google import genai
    client = genai.Client(api_key=GEMINI_API_KEY)
    numbered = "\n".join(f"{i+1}. {c}" for i, c in enumerate(comments))
    prompt = (
        "Bạn là chuyên gia phân tích cảm xúc cho review sản phẩm skincare.\n"
        "Phân loại CẢM XÚC của MỖI review thành MỘT trong ba nhãn: "
        "positive, negative, neutral.\n"
        "Chỉ trả về danh sách nhãn theo đúng thứ tự, mỗi nhãn trên một dòng.\n\n"
        f"Danh sách review:\n{numbered}"
    )
    try:
        response = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
        lines = [ln.strip().lower() for ln in response.text.strip().splitlines() if ln.strip()]
        labels = []
        for ln in lines:
            if "positive" in ln:
                labels.append("positive")
            elif "negative" in ln:
                labels.append("negative")
            else:
                labels.append("neutral")
        if len(labels) != len(comments):
            logger.warning(f"Gemini label count mismatch ({len(labels)} vs {len(comments)}). Padding.")
            labels += ["neutral"] * (len(comments) - len(labels))
            labels = labels[:len(comments)]
        return labels
    except Exception as exc:
        logger.warning(f"Gemini API error: {exc} — defaulting all to 'neutral'")
        return ["neutral"] * len(comments)


def label_sentiment_with_gemini(df: DataFrame) -> DataFrame:
    logger.info("Running Gemini sentiment labeling...")
    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY not set — labeling all as 'neutral'")
        return df.withColumn("sentiment_label", F.lit("neutral"))

    rows = df.select("comment").collect()
    comments = [r["comment"] or "" for r in rows]
    labels = []
    for i in range(0, len(comments), GEMINI_BATCH_SIZE):
        batch = comments[i: i + GEMINI_BATCH_SIZE]
        labels.extend(_label_batch_via_gemini(batch))
        logger.info(f"  Labeled {min(i + GEMINI_BATCH_SIZE, len(comments))}/{len(comments)}")
        if i + GEMINI_BATCH_SIZE < len(comments):
            time.sleep(GEMINI_MIN_INTERVAL)

    from pyspark.sql import Row
    spark = df.sparkSession
    label_df = spark.createDataFrame(
        [Row(_row_idx=idx, sentiment_label=lbl) for idx, lbl in enumerate(labels)]
    )
    indexed = df.withColumn("_row_idx", F.monotonically_increasing_id())
    idx_rows = indexed.select("_row_idx").collect()
    idx_map = {row["_row_idx"]: i for i, row in enumerate(idx_rows)}
    map_df = spark.createDataFrame(
        [Row(_row_idx=orig, _seq=seq) for orig, seq in idx_map.items()]
    )
    return (
        indexed
        .join(map_df, on="_row_idx", how="left")
        .join(label_df.withColumnRenamed("_row_idx", "_seq"), on="_seq", how="left")
        .drop("_row_idx", "_seq")
        .fillna({"sentiment_label": "neutral"})
    )


# ---------------------------------------------------------------------------
# Spark Session — cloud S3/R2
# ---------------------------------------------------------------------------
def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("CustomerAnalyticsPipeline")
        .master("local[2]")
        .config("spark.driver.memory", DRIVER_MEMORY)
        .config("spark.executor.memory", EXECUTOR_MEMORY)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.jars.packages",
            ",".join([
                "org.postgresql:postgresql:42.7.3",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                "io.delta:delta-spark_2.12:3.1.0",
            ]),
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.access.key", AWS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
    )

    if S3_ENDPOINT:
        # Cloudflare R2 or other S3-compatible endpoint
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        )
    else:
        # Standard AWS S3
        builder = builder.config(
            "spark.hadoop.fs.s3a.endpoint",
            f"s3.{AWS_REGION}.amazonaws.com",
        )

    return builder.getOrCreate()


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------
def read_raw(spark: SparkSession) -> DataFrame:
    dfs = []
    for platform, path in [("shopee", RAW_SHOPEE_PATH), ("lazada", RAW_LAZADA_PATH)]:
        try:
            logger.info(f"Reading raw Parquet from: {path}")
            df = spark.read.parquet(path)
            dfs.append(df)
            logger.info(f"  {platform}: {df.count()} rows")
        except Exception as exc:
            logger.warning(f"No Parquet found for {platform}: {exc}")

    if not dfs:
        raise FileNotFoundError(
            f"No raw Parquet files found under s3://{S3_BUCKET}/raw/. "
            "Run scrapers first or check S3 credentials."
        )

    all_cols = sorted({c for df in dfs for c in df.columns})
    aligned = [
        df.select([
            F.col(c) if c in df.columns else F.lit(None).cast("string").alias(c)
            for c in all_cols
        ])
        for df in dfs
    ]
    return reduce(DataFrame.union, aligned)


# ---------------------------------------------------------------------------
# Transform & Quality
# ---------------------------------------------------------------------------
def clean_and_enrich(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("rating", F.col("rating").cast("int"))
        .withColumn("comment", F.trim(F.col("comment")))
        .withColumn("review_date", F.to_date(F.col("review_time")))
        .withColumn("year_month", F.date_format(F.col("review_time"), "yyyy-MM"))
        .dropDuplicates(["platform", "item_id", "reviewer_name", "review_time"])
    )


def check_data_quality(df: DataFrame) -> DataFrame:
    quality_cond = (
        F.col("item_id").isNotNull()
        & F.col("rating").between(1, 5)
        & (F.length(F.col("comment")) > 3)
    )
    good_df = df.filter(quality_cond)
    bad_df  = df.filter(~quality_cond)
    bad_count = bad_df.count()
    if bad_count > 0:
        logger.warning(f"{bad_count} records failed QC → quarantine: {QUARANTINE_PATH}")
        try:
            bad_df.write.format("parquet").mode("append").save(QUARANTINE_PATH)
        except Exception as exc:
            logger.error(f"Quarantine write failed: {exc}")
    return good_df


# ---------------------------------------------------------------------------
# Write Delta + JDBC
# ---------------------------------------------------------------------------
def write_processed(df: DataFrame) -> str:
    logger.info(f"Writing Delta Lake → {DELTA_OUT_PATH}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("platform", "year_month")
        .save(DELTA_OUT_PATH)
    )
    logger.info("Delta write complete.")
    return DELTA_OUT_PATH


def _dedup_against_existing(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Anti-join incoming rows against already-loaded staging.reviews.
    Generates review_key = MD5(platform || item_id::text || reviewer_name || review_time::text)
    matching the surrogate key used by dbt models.
    Only new rows (not in staging) are returned.
    """
    logger.info("Deduplicating against existing staging.reviews...")
    try:
        existing_keys_df = (
            spark.read.format("jdbc")
            .option("url", JDBC_URL)
            .option(
                "query",
                "SELECT DISTINCT MD5(platform || item_id::text || reviewer_name "
                "|| review_time::text) AS review_key FROM staging.reviews",
            )
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASS)
            .option("driver", JDBC_DRIVER)
            .load()
        )
        incoming = df.withColumn(
            "_review_key",
            F.md5(
                F.concat_ws("",
                    F.col("platform"),
                    F.col("item_id").cast("string"),
                    F.col("reviewer_name"),
                    F.col("review_time").cast("string"),
                )
            ),
        )
        deduped = (
            incoming
            .join(
                existing_keys_df.withColumnRenamed("review_key", "_existing_key"),
                incoming["_review_key"] == F.col("_existing_key"),
                how="left_anti",
            )
            .drop("_review_key")
        )
        new_count = deduped.count()
        logger.info(f"Dedup: {df.count()} incoming → {new_count} new rows to insert")
        return deduped
    except Exception as exc:
        logger.warning(f"Dedup failed (inserting all rows): {exc}")
        return df


def load_to_postgres(df: DataFrame, spark: SparkSession | None = None) -> None:
    logger.info(f"JDBC load → {POSTGRES_HOST}/{POSTGRES_DB} :: staging.reviews (mode={JDBC_WRITE_MODE})")

    # In incremental mode, anti-join against existing rows before insert
    if SPARK_MODE == "incremental" and spark is not None:
        df = _dedup_against_existing(df, spark)

    if df.count() == 0:
        logger.info("No new rows to insert — staging.reviews already up to date.")
        return

    (
        df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "staging.reviews")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASS)
        .option("driver", JDBC_DRIVER)
        .option("batchsize", 1000)
        .mode(JDBC_WRITE_MODE)
        .save()
    )
    logger.success("PostgreSQL load complete.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run_pipeline():
    logger.add(sys.stdout, level="INFO", colorize=False)
    logger.info("=" * 60)
    logger.info("Customer Analytics Spark Pipeline — Cloud S3 Mode")
    logger.info(f"  Mode     : {SPARK_MODE}")
    logger.info(f"  Bucket   : {S3_BUCKET}")
    logger.info(f"  Endpoint : {S3_ENDPOINT or 'AWS default'}")
    logger.info(f"  Postgres : {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    logger.info(f"  Today UTC: {TODAY_UTC}")
    logger.info("=" * 60)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df       = read_raw(spark)
    cleaned_df   = clean_and_enrich(raw_df)
    validated_df = check_data_quality(cleaned_df)
    logger.info(f"Clean rows after QC: {validated_df.count()}")

    labeled_df = label_sentiment_with_gemini(validated_df)
    write_processed(labeled_df)
    load_to_postgres(labeled_df, spark)  # pass spark for incremental dedup

    spark.stop()
    logger.success("Pipeline complete ✓")


if __name__ == "__main__":
    run_pipeline()
