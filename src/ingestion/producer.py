import json
import time
import os
import sys
import logging
import redis
from pathlib import Path
from src.ingestion.generator import generate_mock_transaction
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("NexusProducer")

# Kafka Configuration
KAFKA_CONF = {
    'bootstrap.servers': 'broker:29092',
    'enable.idempotence': True,
    'acks': 'all'
}

def get_redis_client():
    try:
        client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
        client.ping()
        return client
    except redis.ConnectionError as e:
        logger.error(f"Redis connection error: {e}")
        raise

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"The message could not be sent: {err}")
    else:
        logger.info(f"The message was sent successfully: Topic: {msg.topic()}, Partition: {msg.partition()}")

def run_producer():
    logger.info("Producer is starting...")
    r = get_redis_client()
    
    try:
        schema_registry_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})

        # Set the schema path to dynamic or static
        current_script_path = Path(__file__).resolve()
        project_root = current_script_path.parent.parent.parent
        schema_path = project_root / "config" / "schemas" / "transaction.avsc"
        
        if not os.path.exists(schema_path):
            logger.error(f"Schema file not found: {schema_path}")
            return

        with open(schema_path, "r") as f:
            schema_str = f.read()
        
        avro_serializer = AvroSerializer(schema_registry_client, schema_str)
        producer = SerializingProducer({**KAFKA_CONF, 'value.serializer': avro_serializer})

        sent_count = 0
        for _ in range(10):
            tx_data = generate_mock_transaction()
            tx_id = tx_data['transaction_id']
            
            # Application-level Idempotency (Redis)
            if not r.exists(tx_id):
                producer.produce(
                    topic='nexus_transactions',
                    value=tx_data,
                    on_delivery=delivery_report
                )
                r.setex(tx_id, 3600, "processed")
                sent_count += 1
            else:
                logger.warning(f"Duplicate data has been blocked: {tx_id}")

        producer.flush()
        logger.info(f"The operation is complete. A total of {sent_count} new messages were sent.")

    except Exception as e:
        logger.error(f"A critical error occurred while Producer was running: {str(e)}")
        raise

if __name__ == "__main__":
    run_producer()