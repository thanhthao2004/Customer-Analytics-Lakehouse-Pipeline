"""
PySpark processing pipeline.

Steps:
1. Read raw Parquet files from data/raw/shopee and data/raw/lazada
2. Clean & normalize
3. Add sentiment_label (positive/negative/neutral) via Google Gemini Flash API
   - Batches 50 reviews per API call to stay within free-tier limits (15 req/min)
   - Falls back to "neutral" on any API error
4. Write processed Parquet to data/processed/
5. Load processed data into PostgreSQL staging tables
"""

import os
import sys
import time
from functools import reduce
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from loguru import logger

# Add project root to path so we can import config
sys.path.insert(0, str(Path(__file__).parent.parent / "ingestion" / "scrapers"))
from config import settings

PROJECT_ROOT = Path(__file__).parent.parent.parent
RAW_DIR = PROJECT_ROOT / settings.RAW_DATA_DIR
PROCESSED_DIR = PROJECT_ROOT / settings.PROCESSED_DATA_DIR
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

JDBC_DRIVER = "org.postgresql.Driver"

# ---------------------------------------------------------------------------
# Gemini sentiment helper (runs on driver, called via pandas UDF)
# ---------------------------------------------------------------------------

GEMINI_MODEL = "gemini-2.0-flash"
GEMINI_BATCH_SIZE = 50          # reviews per API call
GEMINI_MIN_INTERVAL = 4.1       # seconds between calls → ~14 req/min (free tier: 15)


def _label_batch_via_gemini(comments: list[str]) -> list[str]:
    """
    Send a batch of comments to Gemini Flash and return
    a list of labels: 'positive', 'negative', or 'neutral'.

    The prompt is designed for Vietnamese + English mixed text.
    """
    from google import genai

    client = genai.Client(api_key=settings.GEMINI_API_KEY)

    numbered = "\n".join(f"{i+1}. {c}" for i, c in enumerate(comments))
    prompt = (
        "Bạn là chuyên gia phân tích cảm xúc cho review sản phẩm skincare.\n"
        "Dưới đây là danh sách các review được đánh số. "
        "Phân loại CẢM XÚC của MỖI review thành MỘT trong ba nhãn sau: "
        "positive, negative, neutral.\n"
        "Chỉ trả về danh sách nhãn theo đúng thứ tự số, mỗi nhãn trên một dòng, "
        "không giải thích thêm. Ví dụ:\n"
        "positive\nneutral\nnegative\n\n"
        f"Danh sách review:\n{numbered}"
    )

    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
        )
        lines = [ln.strip().lower() for ln in response.text.strip().splitlines() if ln.strip()]
        labels = []
        for ln in lines:
            if "positive" in ln:
                labels.append("positive")
            elif "negative" in ln:
                labels.append("negative")
            else:
                labels.append("neutral")

        # Ensure we have the same count as input
        if len(labels) != len(comments):
            logger.warning(
                f"Gemini returned {len(labels)} labels for {len(comments)} comments. "
                "Padding with 'neutral'."
            )
            labels += ["neutral"] * (len(comments) - len(labels))
            labels = labels[:len(comments)]

        return labels

    except Exception as exc:
        logger.warning(f"Gemini API error: {exc} — defaulting to 'neutral' for batch")
        return ["neutral"] * len(comments)


def label_sentiment_with_gemini(df: DataFrame) -> DataFrame:
    """
    Collect the comment column, batch-call Gemini, and join labels back.
    This runs on the Spark driver (not distributed) which is fine for
    up to ~100k rows; for millions, partition and use pandas UDF instead.
    """
    logger.info("Running Gemini sentiment labeling...")

    if not settings.GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY not set — all labels will be 'neutral'")
        return df.withColumn("sentiment_label", F.lit("neutral"))

    # Collect to driver
    rows = df.select("comment").collect()
    comments = [r["comment"] or "" for r in rows]

    labels = []
    for i in range(0, len(comments), GEMINI_BATCH_SIZE):
        batch = comments[i: i + GEMINI_BATCH_SIZE]
        batch_labels = _label_batch_via_gemini(batch)
        labels.extend(batch_labels)
        logger.info(
            f"  Labeled {min(i + GEMINI_BATCH_SIZE, len(comments))}/{len(comments)} reviews"
        )
        if i + GEMINI_BATCH_SIZE < len(comments):
            time.sleep(GEMINI_MIN_INTERVAL)

    # Attach row index so we can join back
    from pyspark.sql import Row

    spark = df.sparkSession
    label_df = spark.createDataFrame(
        [Row(_row_idx=idx, sentiment_label=lbl) for idx, lbl in enumerate(labels)]
    )

    # Add monotonically increasing row index to original df
    indexed = df.withColumn("_row_idx", F.monotonically_increasing_id())

    # Remap monotonic IDs to 0-based by collecting and reassigning
    # (monotonically_increasing_id is not guaranteed to be 0-based)
    idx_rows = indexed.select("_row_idx").collect()
    idx_map = {row["_row_idx"]: i for i, row in enumerate(idx_rows)}

    # Build a broadcast-friendly mapping df
    map_df = spark.createDataFrame(
        [Row(_row_idx=orig, _seq=seq) for orig, seq in idx_map.items()]
    )

    result = (
        indexed
        .join(map_df, on="_row_idx", how="left")
        .join(label_df.withColumnRenamed("_row_idx", "_seq"), on="_seq", how="left")
        .drop("_row_idx", "_seq")
        .fillna({"sentiment_label": "neutral"})
    )
    return result


# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------

def create_spark_session() -> SparkSession:
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    if not minio_endpoint.startswith("http"):
        minio_endpoint = f"http://{minio_endpoint}"

    return (
        SparkSession.builder.appName("CustomerAnalyticsPipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.1.0")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "admin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "adminpassword"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------

def read_raw(spark: SparkSession) -> DataFrame:
    """Read all Parquet files from raw shopee + lazada dirs."""
    dfs = []
    for platform in ["shopee", "lazada"]:
        path_pattern = f"s3a://raw-data/{platform}/*.parquet"
        try:
            logger.info(f"Reading raw data from MinIO S3: {path_pattern}")
            df = spark.read.parquet(path_pattern)
            dfs.append(df)
        except Exception as e:
            logger.warning(f"No parquet files found or accessible for {platform} at {path_pattern}: {e}")

    if not dfs:
        raise FileNotFoundError(
            f"No raw Parquet files found under {RAW_DIR}. Run scrapers first."
        )

    def union_all(dfs: list) -> DataFrame:
        all_cols = sorted({c for df in dfs for c in df.columns})
        aligned = [
            df.select([
                F.col(c) if c in df.columns else F.lit(None).cast("string").alias(c)
                for c in all_cols
            ])
            for df in dfs
        ]
        return reduce(DataFrame.union, aligned)

    return union_all(dfs)


# ---------------------------------------------------------------------------
# Data Quality & Transform
# ---------------------------------------------------------------------------

def clean_and_enrich(df: DataFrame) -> DataFrame:
    """Clean nulls, normalise types, remove empty reviews."""
    df = (
        df
        .withColumn("rating", F.col("rating").cast("int"))
        .withColumn("comment", F.trim(F.col("comment")))
        .withColumn("review_date", F.to_date(F.col("review_time")))
        .withColumn("year_month", F.date_format(F.col("review_time"), "yyyy-MM"))
        .dropDuplicates(["platform", "item_id", "reviewer_name", "review_time"])
    )
    return df

def check_data_quality(df: DataFrame) -> DataFrame:
    """
    Enforce Data Quality by verifying:
    1. Rating is between 1 and 5
    2. Comment length > 3
    3. Item ID is not null
    Quarantines bad records and returns only good records.
    """
    quality_cond = (
        F.col("item_id").isNotNull() &
        F.col("rating").between(1, 5) &
        (F.length(F.col("comment")) > 3)
    )
    
    # Split records
    good_df = df.filter(quality_cond)
    bad_df = df.filter(~quality_cond)
    
    bad_count = bad_df.count()
    if bad_count > 0:
        logger.warning(f"Data Quality Check Failed for {bad_count} records. Moving to Quarantine S3 bucket.")
        quarantine_path = "s3a://raw-data/quarantine_reviews"
        try:
            bad_df.write.format("parquet").mode("append").save(quarantine_path)
            logger.info(f"Quarantined {bad_count} records to {quarantine_path}")
        except Exception as e:
            logger.error(f"Failed to write quarantine data: {e}")
            
    return good_df


# ---------------------------------------------------------------------------
# Write Processed Parquet
# ---------------------------------------------------------------------------

def write_processed(df: DataFrame) -> str:
    out = "s3a://raw-data/processed_delta"
    logger.info(f"Writing processed Delta tables to MinIO -> {out}")
    df.write.format("delta").mode("overwrite").partitionBy("platform", "year_month").save(out)
    return out


# ---------------------------------------------------------------------------
# Load into PostgreSQL (staging layer)
# ---------------------------------------------------------------------------

def load_to_postgres(df: DataFrame) -> None:
    logger.info("Loading data into PostgreSQL staging.reviews ...")
    (
        df.write.format("jdbc")
        .option("url", settings.postgres_jdbc_url)
        .option("dbtable", "staging.reviews")
        .option("user", settings.POSTGRES_USER)
        .option("password", settings.POSTGRES_PASSWORD)
        .option("driver", JDBC_DRIVER)
        .option("batchsize", 1000)
        .mode("append")
        .save()
    )
    logger.success("PostgreSQL load complete.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_pipeline():
    logger.add(
        PROJECT_ROOT / settings.LOGS_DIR / "spark_{time}.log",
        rotation="50 MB",
        level="INFO",
    )
    logger.info("Starting Spark pipeline...")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = read_raw(spark)
    logger.info(f"Raw rows: {raw_df.count()}")

    cleaned_df = clean_and_enrich(raw_df)
    
    # Apply Data Quality Constraints
    validated_df = check_data_quality(cleaned_df)
    logger.info(f"Validated clean rows: {validated_df.count()}")

    # Gemini sentiment labeling (driver-side batching)
    labeled_df = label_sentiment_with_gemini(validated_df)

    write_processed(labeled_df)
    load_to_postgres(labeled_df)

    spark.stop()
    logger.success("Pipeline complete.")


if __name__ == "__main__":
    run_pipeline()
