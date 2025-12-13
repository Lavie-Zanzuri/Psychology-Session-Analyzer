import pika
import json
import logging
import os
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "admin")
RABBITMQ_QUEUE = "video_uploaded"

def callback(ch, method, properties, body):
    """Process received message"""
    try:
        message = json.loads(body)
        logger.info("=" * 50)
        logger.info("📨 Received event from RabbitMQ:")
        logger.info(f"   Event Type: {message.get('event_type')}")
        logger.info(f"   Filename: {message.get('filename')}")
        logger.info(f"   Bucket: {message.get('bucket')}")
        logger.info(f"   Size: {message.get('size')} bytes")
        logger.info(f"   Timestamp: {message.get('timestamp')}")
        logger.info("=" * 50)
        
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing message: {e}", exc_info=True)

def main():
    """Start consuming messages"""
    while True:
        try:
            logger.info("Connecting to RabbitMQ...")
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            # Declare queue
            channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
            
            logger.info(f"✅ Connected! Waiting for messages on queue: {RABBITMQ_QUEUE}")
            logger.info("To exit press CTRL+C")
            
            # Start consuming
            channel.basic_consume(
                queue=RABBITMQ_QUEUE,
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