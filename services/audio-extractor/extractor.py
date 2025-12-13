import pika
import json
import logging
import os
import subprocess
from minio import Minio
from minio.error import S3Error
import tempfile
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "psychology-sessions")

# RabbitMQ configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "admin")
INPUT_QUEUE = "video_uploaded"
OUTPUT_QUEUE = "audio_ready"

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
        channel.queue_declare(queue=OUTPUT_QUEUE, durable=True)
        
        # Send message
        channel.basic_publish(
            exchange='',
            routing_key=OUTPUT_QUEUE,
            body=json.dumps(event_data),
            properties=pika.BasicProperties(
                delivery_mode=2,
            )
        )
        
        logger.info(f"✅ Sent event to RabbitMQ: {event_data}")
        connection.close()
    except Exception as e:
        logger.error(f"Failed to send event to RabbitMQ: {e}", exc_info=True)

def extract_audio(video_filename):
    """Extract audio from video and convert to MP3"""
    logger.info(f"🎬 Processing video: {video_filename}")
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        video_path = os.path.join(temp_dir, "input_video.mp4")
        audio_path = os.path.join(temp_dir, "output_audio.mp3")
        
        try:
            # Download video from MinIO
            logger.info(f"📥 Downloading {video_filename} from MinIO...")
            minio_client.fget_object(MINIO_BUCKET, video_filename, video_path)
            logger.info(f"✅ Downloaded successfully")
            
            # Extract audio using ffmpeg
            logger.info(f"🎵 Extracting audio with ffmpeg...")
            command = [
                'ffmpeg',
                '-i', video_path,
                '-vn',  # No video
                '-acodec', 'libmp3lame',  # MP3 codec
                '-b:a', '192k',  # Bitrate
                '-y',  # Overwrite output file
                audio_path
            ]
            
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True
            )
            
            logger.info(f"✅ Audio extraction completed")
            
            # Generate MP3 filename
            base_name = os.path.splitext(video_filename)[0]
            mp3_filename = f"{base_name}.mp3"
            
            # Upload MP3 to MinIO
            logger.info(f"📤 Uploading {mp3_filename} to MinIO...")
            minio_client.fput_object(
                MINIO_BUCKET,
                mp3_filename,
                audio_path,
                content_type='audio/mpeg'
            )
            logger.info(f"✅ Uploaded successfully")
            
            # Get file size
            audio_size = os.path.getsize(audio_path)
            
            return mp3_filename, audio_size
            
        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg error: {e.stderr}", exc_info=True)
            raise
        except S3Error as e:
            logger.error(f"MinIO error: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Error extracting audio: {e}", exc_info=True)
            raise

def callback(ch, method, properties, body):
    """Process video_uploaded event"""
    try:
        message = json.loads(body)
        logger.info("=" * 60)
        logger.info("📨 Received video_uploaded event")
        logger.info(f"   Filename: {message.get('filename')}")
        logger.info("=" * 60)
        
        video_filename = message.get('filename')
        
        # Extract audio
        mp3_filename, audio_size = extract_audio(video_filename)
        
        # Send audio_ready event
        event_data = {
            "event_type": "audio_ready",
            "video_filename": video_filename,
            "audio_filename": mp3_filename,
            "bucket": MINIO_BUCKET,
            "size": audio_size,
            "timestamp": datetime.now().isoformat()
        }
        send_event(event_data)
        
        logger.info("=" * 60)
        logger.info("✅ Audio extraction completed successfully!")
        logger.info(f"   MP3 file: {mp3_filename}")
        logger.info("=" * 60)
        
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        logger.error(f"❌ Error processing message: {e}", exc_info=True)
        # Don't acknowledge - message will be requeued
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    """Start consuming messages"""
    import time
    
    while True:
        try:
            logger.info("🔌 Connecting to RabbitMQ...")
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            
            # Declare queue
            channel.queue_declare(queue=INPUT_QUEUE, durable=True)
            
            # Set prefetch to 1 - process one message at a time
            channel.basic_qos(prefetch_count=1)
            
            logger.info(f"✅ Connected! Waiting for messages on queue: {INPUT_QUEUE}")
            logger.info("🎵 Audio Extractor Service is running...")
            logger.info("To exit press CTRL+C")
            
            # Start consuming
            channel.basic_consume(
                queue=INPUT_QUEUE,
                on_message_callback=callback
            )
            
            channel.start_consuming()
            
        except KeyboardInterrupt:
            logger.info("Interrupted by user, shutting down...")
            break
        except Exception as e:
            logger.error(f"Connection error: {e}")
            logger.info("Retrying in 5 seconds...")
            time.sleep(5)

if __name__ == "__main__":
    main()