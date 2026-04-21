# Role: Senior Data Engineer (Architecture & Pipelines)

## Infrastructure Design
This project implements a decoupled, dual-compute infrastructure designed specifically to circumvent external firewall blocks (Datadome/Cloudflare) while running heavy big-data workloads resiliently.

* **Mac Host (Extraction Compute):** Handles visual headless browsers using `undetected_chromedriver`. It executes chunked Parquet persistence incrementally into RAM-safe files to prevent Stale Element Exceptions on heavily dynamic React DOM trees.
* **Ubuntu VM (Data Infrastructure Hub):** Hosts a customized Docker Compose stack orchestrating PostgreSQL, MinIO, Airflow, and Metabase autonomously.

## System Architecture Alignment (Mapping to pipeline-system.png)

1. **Ingestion Layer (Scraping -> S3)**
   * Built programmatic loop handlers injecting native JavaScript directly into the DOM, scraping thousands of nested reviews in O(1) time complexity.
   * `upload_to_minio.py` utilizes the Boto3 API to safely transport massive Parquet blocks natively across the local network boundary into the `s3a://raw-data` MinIO bucket.

2. **Processing Layer (Spark + Delta Lake)**
   * Configured native PySpark sessions integrating robust Hive/Hadoop AWS configs, directly querying the S3 bucket via `org.apache.hadoop:hadoop-aws`.
   * **Data Observability:** Embedded `check_data_quality()` interceptors to assert required IDs and rating ranges, routing any corrupted records to an explicit "Dead Letter" `quarantine_reviews` bucket.
   * **Transformation:** Fused Big Data and AI by batch-requesting Gemini Flash API for rapid, precise multi-lingual NLP analysis.
   * **Lakehouse Storage:** Re-formats raw Parquet frames into explicit **Delta Lake** formats (`io.delta.sql.DeltaSparkSessionExtension`), providing actual ACID transactions backing the S3 data block layer.

3. **Orchestration Layer (Airflow Event-Driven Automation)**
   * Avoided static `cron` brittle designs. Developed DAGs deploying `S3KeySensor` capabilities that continuously ping MinIO, automatically triggering the Spark Submit scripts the exact millisecond the Mac Host completes an upload batch.

4. **CI/CD Code Quality (GitHub Actions)**
   * Integrated automated Python type linting pipelines executing `flake8` against `__main__` builds. Handled deep Pyre type hints correctly defining object signatures around dynamic driver instances to guarantee zero-defect deployment pipelines.

## Scalability Profiler
By designing this as a multi-tenant layout (using Delta Lake, Airflow triggers, and separated Database volumes), this pipeline can scale linearly. We can simply spin up extra Docker nodes or migrate MinIO effortlessly to AWS S3 / Databricks without changing a single line of business logic.
