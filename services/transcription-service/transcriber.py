import pika
import json
import logging
import os
from minio import Minio
from minio.error import S3Error
import assemblyai as aai
from datetime import datetime
import time

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
INPUT_QUEUE = "audio_ready"
OUTPUT_QUEUE = "transcription_ready"

# AssemblyAI configuration
ASSEMBLYAI_API_KEY = os.getenv("ASSEMBLYAI_API_KEY")
aai.settings.api_key = ASSEMBLYAI_API_KEY

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
        
        channel.queue_declare(queue=OUTPUT_QUEUE, durable=True)
        
        channel.basic_publish(
            exchange='',
            routing_key=OUTPUT_QUEUE,
            body=json.dumps(event_data),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        
        logger.info(f"✅ Sent event to RabbitMQ: {event_data['event_type']}")
        connection.close()
    except Exception as e:
        logger.error(f"Failed to send event to RabbitMQ: {e}", exc_info=True)

def get_presigned_url(filename):
    """Get presigned URL for MinIO object"""
    try:
        from datetime import timedelta
        url = minio_client.presigned_get_object(
            MINIO_BUCKET,
            filename,
            expires=timedelta(hours=1)
        )
        logger.info(f"Generated presigned URL for {filename}")
        return url
    except Exception as e:
        logger.error(f"Error generating presigned URL: {e}", exc_info=True)
        raise

def transcribe_audio(audio_filename):
    """Transcribe audio using AssemblyAI with speaker diarization"""
    logger.info(f"🎙️ Starting transcription: {audio_filename}")
    
    try:
        # Download audio file from MinIO to local temp file
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix='.mp3') as tmp_file:
            tmp_path = tmp_file.name
            
        logger.info(f"📥 Downloading {audio_filename} from MinIO...")
        minio_client.fget_object(MINIO_BUCKET, audio_filename, tmp_path)
        logger.info(f"✅ Downloaded to {tmp_path}")
        
        # Configure transcription with speaker diarization
        config = aai.TranscriptionConfig(
            speaker_labels=True,  # Enable speaker diarization
            speakers_expected=2   # Expect 2 speakers (therapist + patient)
        )
        
        # Create transcriber
        transcriber = aai.Transcriber()
        
        logger.info(f"⏳ Submitting to AssemblyAI...")
        transcript = transcriber.transcribe(tmp_path, config)
        
        # Clean up temp file
        import os
        os.remove(tmp_path)
        logger.info(f"🗑️ Cleaned up temp file")
        
        # Wait for completion
        logger.info(f"⏳ Waiting for transcription to complete...")
        
        if transcript.status == aai.TranscriptStatus.error:
            raise Exception(f"Transcription failed: {transcript.error}")
        
        logger.info(f"✅ Transcription completed!")
        logger.info(f"📝 Total utterances: {len(transcript.utterances) if transcript.utterances else 0}")
        
        # Format the transcription data
        transcription_data = {
            "text": transcript.text,
            "utterances": []
        }
        
        # Add speaker-separated utterances
        if transcript.utterances:
            for utterance in transcript.utterances:
                transcription_data["utterances"].append({
                    "speaker": utterance.speaker,
                    "text": utterance.text,
                    "start": utterance.start,
                    "end": utterance.end,
                    "confidence": utterance.confidence
                })
        
        # Save transcription to MinIO as JSON
        transcription_filename = audio_filename.replace('.mp3', '_transcription.json')
        
        import io
        transcription_json = json.dumps(transcription_data, indent=2)
        transcription_bytes = transcription_json.encode('utf-8')
        
        minio_client.put_object(
            MINIO_BUCKET,
            transcription_filename,
            io.BytesIO(transcription_bytes),
            len(transcription_bytes),
            content_type='application/json'
        )
        
        logger.info(f"💾 Saved transcription to MinIO: {transcription_filename}")
        
        return transcription_filename, transcription_data
        
    except Exception as e:
        logger.error(f"Error transcribing audio: {e}", exc_info=True)
        raise

def callback(ch, method, properties, body):
    """Process audio_ready event"""
    try:
        message = json.loads(body)
        logger.info("=" * 60)
        logger.info("📨 Received audio_ready event")
        logger.info(f"   Audio file: {message.get('audio_filename')}")
        logger.info("=" * 60)
        
        audio_filename = message.get('audio_filename')
        video_filename = message.get('video_filename')
        
        # Transcribe audio
        transcription_filename, transcription_data = transcribe_audio(audio_filename)
        
        # Send transcription_ready event
        event_data = {
            "event_type": "transcription_ready",
            "video_filename": video_filename,
            "audio_filename": audio_filename,
            "transcription_filename": transcription_filename,
            "bucket": MINIO_BUCKET,
            "utterances_count": len(transcription_data.get("utterances", [])),
            "timestamp": datetime.now().isoformat()
        }
        send_event(event_data)
        
        logger.info("=" * 60)
        logger.info("✅ Transcription completed successfully!")
        logger.info(f"   Transcription file: {transcription_filename}")
        logger.info(f"   Utterances: {len(transcription_data.get('utterances', []))}")
        logger.info("=" * 60)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        logger.error(f"❌ Error processing message: {e}", exc_info=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    """Start consuming messages"""
    while True:
        try:
            logger.info("🔌 Connecting to RabbitMQ...")
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            
            channel.queue_declare(queue=INPUT_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)
            
            logger.info(f"✅ Connected! Waiting for messages on queue: {INPUT_QUEUE}")
            logger.info("🎙️ Transcription Service is running...")
            logger.info("To exit press CTRL+C")
            
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