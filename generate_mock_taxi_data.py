import json
import os
import time
import random
from datetime import datetime

# Build a local directory to hold the sample documents
os.makedirs("./local_staging", exist_ok=True)

# Generate a unique unix timestamp identifier to prevent name collisions
current_timestamp = int(time.time())

# Format a dynamic data block mimicking the NYC Taxi schema structure
mock_record = {
    "vendor_id": str(random.randint(1, 2)),
    "tpep_pickup_datetime": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    "passenger_count": random.randint(1, 4),
    "trip_distance": round(random.uniform(1.0, 10.0), 2),
    "fare_amount": round(random.uniform(5.0, 50.0), 2),
    "pickup_zip": str(random.choice([10001, 10002, 10003, 10027, 11201]))
}

# Generate a single completely unique file per script execution call
file_name = f"taxi_stream_{current_timestamp}.json"

with open(f"./local_staging/{file_name}", "w") as f:
    json.dump(mock_record, f)

print(f"Successfully generated unique dynamic file: {file_name}")
