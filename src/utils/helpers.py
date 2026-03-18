import urllib.parse

# AWS Credentials
AWS_ACCESS_KEY = "access_key"
AWS_SECRET_KEY = "secret_key"
AWS_BUCKET_NAME = "bucket_name"

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "ngrok_kafka_url"
KAFKA_TOPIC = "nexus_transactions"

# S3 Path Calculation (Base Path)
encoded_secret = urllib.parse.quote(AWS_SECRET_KEY, safe='')
BASE_S3_PATH = f"s3a://{AWS_ACCESS_KEY}:{encoded_secret}@{AWS_BUCKET_NAME}"

# Layer Paths
BRONZE_PATH = f"{BASE_S3_PATH}/bronze/transactions"
SILVER_PATH = f"{BASE_S3_PATH}/silver/transactions"
GOLD_CUSTOMER_PATH = f"{BASE_S3_PATH}/gold/customer_spending_summary"
GOLD_CURRENCY_PATH = f"{BASE_S3_PATH}/gold/currency_stats"

# Checkpoint Paths
CHECKPOINT_BRONZE = f"{BASE_S3_PATH}/checkpoints/bronze_v1"
CHECKPOINT_SILVER = f"{BASE_S3_PATH}/checkpoints/silver_v1"
CHECKPOINT_GOLD = f"{BASE_S3_PATH}/checkpoints/gold_v1"