from fastapi import FastAPI, UploadFile, File, HTTPException
from minio import Minio
from minio.error import S3Error
import os
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Upload Service")

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "psychology-sessions")

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

@app.on_event("startup")
async def startup_event():
    """Ensure bucket exists on startup"""
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
            logger.info(f"Created bucket: {MINIO_BUCKET}")
        else:
            logger.info(f"Bucket {MINIO_BUCKET} already exists")
    except S3Error as e:
        logger.error(f"Error checking/creating bucket: {e}")

@app.get("/")
async def root():
    return {"message": "Upload Service is running"}

@app.post("/upload")
async def upload_video(file: UploadFile = File(...)):
    """Upload a video file to MinIO"""
    try:
        # Validate file type
        if not file.content_type.startswith('video/'):
            raise HTTPException(status_code=400, detail="File must be a video")
        
        # Generate unique filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        original_filename = file.filename
        file_extension = os.path.splitext(original_filename)[1]
        unique_filename = f"{timestamp}_{original_filename}"
        
        logger.info(f"Uploading file: {unique_filename}")
        
        # Upload to MinIO
        file_data = await file.read()
        file_size = len(file_data)
        
        from io import BytesIO
        minio_client.put_object(
            MINIO_BUCKET,
            unique_filename,
            BytesIO(file_data),
            file_size,
            content_type=file.content_type
        )
        
        logger.info(f"Successfully uploaded {unique_filename} ({file_size} bytes)")
        
        return {
            "message": "File uploaded successfully",
            "filename": unique_filename,
            "size": file_size,
            "bucket": MINIO_BUCKET
        }
        
    except S3Error as e:
        logger.error(f"MinIO error: {e}")
        raise HTTPException(status_code=500, detail=f"MinIO error: {str(e)}")
    except Exception as e:
        logger.error(f"Upload error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Check MinIO connection
        minio_client.bucket_exists(MINIO_BUCKET)
        return {"status": "healthy", "minio": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}