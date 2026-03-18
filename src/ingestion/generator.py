import uuid
import random
import time

def generate_mock_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user-{random.randint(100, 999)}",
        "amount": round(random.uniform(10.0, 5000.0), 2),
        "currency": random.choice(["USD", "EUR", "TRY"]),
        "timestamp": int(time.time()),
        "source_system": random.choice(["mobile_app", "web_pos", "api_gateway"]),
        "status": "completed"
    }