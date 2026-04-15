import os
from pathlib import Path
from loguru import logger
from minio import Minio
from minio.error import S3Error
import glob

# Configuration for MinIO
# Since MinIO is running in the Ubuntu VM via Docker, 
# you should normally point to the VM's IP address (e.g., 192.168.x.x:9000).
# If port 9000 is forwarded to localhost, you can use "localhost:9000".
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "adminpassword")
BUCKET_NAME = "raw-data"

PROJECT_ROOT = Path(__file__).parent.parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"

def upload_folder_to_minio():
    """Uploads locally scraped Parquet files to the MinIO Data Lake."""
    # 1. Initialize MinIO Client
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # True if using HTTPS
    )

    # 2. Ensure bucket exists
    try:
        if not client.bucket_exists(BUCKET_NAME):
            logger.info(f"Bucket '{BUCKET_NAME}' does not exist. Creating it...")
            client.make_bucket(BUCKET_NAME)
        else:
            logger.info(f"Bucket '{BUCKET_NAME}' already exists.")
    except S3Error as e:
        logger.error(f"MinIO connection error: {e}")
        logger.warning(
            "If MinIO is running inside the Ubuntu VM, make sure your Mac "
            "can reach the VM's port 9000. Try setting MINIO_ENDPOINT='<VM_IP>:9000'"
        )
        return

    # 3. Upload Parquet files for each platform
    for platform in ["shopee", "lazada"]:
        platform_dir = RAW_DIR / platform
        if not platform_dir.exists():
            continue

        parquet_files = glob.glob(f"{platform_dir}/*.parquet")
        for file_path in parquet_files:
            file_name = Path(file_path).name
            object_name = f"{platform}/{file_name}"

            try:
                # Upload the file
                client.fput_object(
                    BUCKET_NAME,
                    object_name,
                    file_path
                )
                logger.success(f"Uploaded {file_path} to s3://{BUCKET_NAME}/{object_name}")
            except S3Error as err:
                logger.error(f"Failed to upload {file_name}: {err}")

if __name__ == "__main__":
    logger.add(
        PROJECT_ROOT / "logs" / "minio_upload_{time}.log",
        rotation="10 MB",
        level="INFO",
    )
    logger.info("Starting Data Ingestion Layer: Local to MinIO")
    upload_folder_to_minio()
    logger.info("Upload proces