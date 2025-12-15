import pika
import json
import logging
import os
from minio import Minio
from pymongo import MongoClient
import redis
from openai import OpenAI
from datetime import datetime
import time
import hashlib

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
INPUT_QUEUE = "transcription_ready"

# MongoDB configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
MONGO_DB = "psychology_analyzer"

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# OpenAI configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
from openai import OpenAI
openai_client = OpenAI(api_key=OPENAI_API_KEY)

# Initialize clients
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]
analyses_collection = db["analyses"]

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

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

def get_cached_or_call_llm(prompt, cache_key_prefix="llm"):
    """Check Redis cache, if not found call LLM and cache result"""
    # Create cache key from prompt
    cache_key = f"{cache_key_prefix}:{hashlib.md5(prompt.encode()).hexdigest()}"
    
    # Check cache
    cached_result = redis_client.get(cache_key)
    if cached_result:
        logger.info(f"✅ Cache hit for {cache_key_prefix}")
        return cached_result
    
    logger.info(f"⏳ Cache miss, calling OpenAI GPT-4o-mini...")
    
    # Call OpenAI
    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that analyzes therapy sessions. Always respond with valid JSON only, no markdown."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.3
    )
    
    result = response.choices[0].message.content
    
    # Cache for 7 days
    redis_client.setex(cache_key, 604800, result)
    
    return result

def analyze_transcription(transcription_data):
    """Analyze transcription using Gemini"""
    logger.info(f"🧠 Starting analysis...")
    
    utterances = transcription_data.get("utterances", [])
    
    if not utterances:
        raise Exception("No utterances found in transcription")
    
    logger.info(f"📝 Analyzing {len(utterances)} utterances...")
    
    # Step 1: Identify therapist vs patient
    logger.info("👥 Identifying speakers...")
    
    sample_utterances = utterances[:10]  # First 10 for identification
    sample_text = "\n".join([f"Speaker {u['speaker']}: {u['text']}" for u in sample_utterances])
    
    identification_prompt = f"""Given this psychology therapy session transcript, identify which speaker is the THERAPIST and which is the PATIENT.

Transcript sample:
{sample_text}

Respond ONLY with JSON in this exact format (no markdown, no explanation):
{{"therapist": "A", "patient": "B"}}
or
{{"therapist": "B", "patient": "A"}}"""

    identification_result = get_cached_or_call_llm(identification_prompt, "speaker_id")
    
    try:
        # Clean response - remove markdown if present
        clean_result = identification_result.strip()
        if clean_result.startswith("```"):
            clean_result = clean_result.split("\n", 1)[1].rsplit("\n", 1)[0]
        
        speaker_mapping = json.loads(clean_result)
        therapist_speaker = speaker_mapping["therapist"]
        patient_speaker = speaker_mapping["patient"]
        logger.info(f"✅ Identified - Therapist: Speaker {therapist_speaker}, Patient: Speaker {patient_speaker}")
    except Exception as e:
        logger.error(f"Error parsing speaker identification: {e}")
        # Default fallback
        therapist_speaker = "A"
        patient_speaker = "B"
        logger.warning(f"Using default mapping - Therapist: A, Patient: B")
    
    # Step 2: Analyze each utterance for emotion and topic
    logger.info("🎭 Analyzing emotions and topics...")
    
    analyzed_utterances = []
    
    # Process in batches to reduce API calls
    batch_size = 5
    for i in range(0, len(utterances), batch_size):
        batch = utterances[i:i+batch_size]
        
        batch_text = "\n".join([
            f"{idx}. Speaker {u['speaker']}: {u['text']}" 
            for idx, u in enumerate(batch, start=i+1)
        ])
        
        analysis_prompt = f"""Analyze these therapy session utterances. For each one, identify:
1. Emotion: happy, sad, angry, anxious, neutral, confused, hopeful
2. Topic: mother, father, family, work, relationship, childhood, trauma, self, health, future, other

Utterances:
{batch_text}

Respond ONLY with JSON array (no markdown):
[
  {{"utterance": 1, "emotion": "emotion_here", "topic": "topic_here"}},
  {{"utterance": 2, "emotion": "emotion_here", "topic": "topic_here"}}
]"""

        batch_result = get_cached_or_call_llm(analysis_prompt, f"analysis_batch_{i}")
        
        try:
            # Clean response
            clean_result = batch_result.strip()
            if clean_result.startswith("```"):
                clean_result = clean_result.split("\n", 1)[1].rsplit("\n", 1)[0]
            
            batch_analyses = json.loads(clean_result)
            
            for j, analysis in enumerate(batch_analyses):
                utterance = batch[j]
                analyzed_utterances.append({
                    "speaker": utterance["speaker"],
                    "role": "therapist" if utterance["speaker"] == therapist_speaker else "patient",
                    "text": utterance["text"],
                    "start": utterance["start"],
                    "end": utterance["end"],
                    "emotion": analysis.get("emotion", "neutral"),
                    "topic": analysis.get("topic", "other")
                })
        except Exception as e:
            logger.error(f"Error parsing batch analysis: {e}")
            # Add without analysis
            for utterance in batch:
                analyzed_utterances.append({
                    "speaker": utterance["speaker"],
                    "role": "therapist" if utterance["speaker"] == therapist_speaker else "patient",
                    "text": utterance["text"],
                    "start": utterance["start"],
                    "end": utterance["end"],
                    "emotion": "neutral",
                    "topic": "other"
                })
        
        logger.info(f"Processed {min(i+batch_size, len(utterances))}/{len(utterances)} utterances")
    
    # Step 3: Generate insights
    logger.info("💡 Generating insights...")
    
    patient_utterances = [u for u in analyzed_utterances if u["role"] == "patient"]
    
    # Analyze which topics make patient feel good/bad
    insights_prompt = f"""Based on this therapy session analysis, provide insights about what makes the patient feel good vs bad.

Patient utterances with emotions and topics:
{json.dumps(patient_utterances[:20], indent=2)}

Provide:
1. Topics associated with positive emotions (happy, hopeful)
2. Topics associated with negative emotions (sad, angry, anxious)
3. Key recommendations for the therapist

Respond ONLY with JSON (no markdown):
{{
  "positive_topics": ["topic1", "topic2"],
  "negative_topics": ["topic1", "topic2"],
  "recommendations": ["recommendation1", "recommendation2", "recommendation3"]
}}"""

    insights_result = get_cached_or_call_llm(insights_prompt, "insights")
    
    try:
        clean_result = insights_result.strip()
        if clean_result.startswith("```"):
            clean_result = clean_result.split("\n", 1)[1].rsplit("\n", 1)[0]
        
        insights = json.loads(clean_result)
    except Exception as e:
        logger.error(f"Error parsing insights: {e}")
        insights = {
            "positive_topics": [],
            "negative_topics": [],
            "recommendations": ["Continue monitoring patient's emotional state"]
        }
    
    logger.info("✅ Analysis complete!")
    
    return {
        "speaker_mapping": {
            "therapist": therapist_speaker,
            "patient": patient_speaker
        },
        "analyzed_utterances": analyzed_utterances,
        "insights": insights,
        "summary": {
            "total_utterances": len(analyzed_utterances),
            "patient_utterances": len(patient_utterances),
            "therapist_utterances": len(analyzed_utterances) - len(patient_utterances)
        }
    }

def callback(ch, method, properties, body):
    """Process transcription_ready event"""
    try:
        message = json.loads(body)
        logger.info("=" * 60)
        logger.info("📨 Received transcription_ready event")
        logger.info(f"   Transcription file: {message.get('transcription_filename')}")
        logger.info("=" * 60)
        
        transcription_filename = message.get('transcription_filename')
        video_filename = message.get('video_filename')
        audio_filename = message.get('audio_filename')
        
        # Download transcription from MinIO
        logger.info(f"📥 Downloading transcription from MinIO...")
        
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.json') as tmp_file:
            tmp_path = tmp_file.name
        
        minio_client.fget_object(MINIO_BUCKET, transcription_filename, tmp_path)
        
        with open(tmp_path, 'r') as f:
            transcription_data = json.load(f)
        
        import os
        os.remove(tmp_path)
        
        logger.info(f"✅ Transcription loaded")
        
        # Analyze
        analysis_result = analyze_transcription(transcription_data)
        
        # Save to MongoDB
        logger.info(f"💾 Saving analysis to MongoDB...")
        
        analysis_document = {
            "video_filename": video_filename,
            "audio_filename": audio_filename,
            "transcription_filename": transcription_filename,
            "analysis": analysis_result,
            "created_at": datetime.now(),
            "bucket": MINIO_BUCKET
        }
        
        result = analyses_collection.insert_one(analysis_document)
        analysis_id = str(result.inserted_id)
        
        logger.info(f"✅ Saved with ID: {analysis_id}")
        
        logger.info("=" * 60)
        logger.info("✅ Analysis completed successfully!")
        logger.info(f"   Total utterances: {analysis_result['summary']['total_utterances']}")
        logger.info(f"   Patient utterances: {analysis_result['summary']['patient_utterances']}")
        logger.info(f"   Recommendations: {len(analysis_result['insights']['recommendations'])}")
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
            logger.info("🧠 Analysis Service is running...")
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