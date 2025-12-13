from fastapi import FastAPI, UploadFile, File, HTTPException
from minio import Minio
from minio.error import S3Error
import os
from datetime import datetime
import logging
import pika
import json

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Upload Service")

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "psychology-sessions")

# RabbitMQ configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "admin")
RABBITMQ_QUEUE = "video_uploaded"

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def get_rabbitmq_connection():
    """Create RabbitMQ connection"""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
    return pika.BlockingConnection(parameters)

def send_event(event_data):
    """Send event to RabbitMQ"""
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        
        # Declare queue
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        
        # Send message
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(event_data),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )
        
        logger.info(f"Sent event to RabbitMQ: {event_data}")
        connection.close()
    except Exception as e:
        logger.error(f"Failed to send event to RabbitMQ: {e}", exc_info=True)

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
    """Upload a video file to MinIO and send event to RabbitMQ"""
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
        
        # Send event to RabbitMQ
        event_data = {
            "event_type": "video_uploaded",
            "filename": unique_filename,
            "bucket": MINIO_BUCKET,
            "size": file_size,
            "timestamp": datetime.now().isoformat()
        }
        send_event(event_data)
        
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
        logger.error(f"Upload error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Check MinIO connection
        minio_client.bucket_exists(MINIO_BUCKET)
        
        # Check RabbitMQ connection
        connection = get_rabbitmq_connection()
        connection.close()
        
        return {"status": "healthy", "minio": "connected", "rabbitmq": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}