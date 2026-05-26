import json
import os
from datetime import datetime

# Build a local directory to hold the sample documents
os.makedirs("./local_staging", exist_ok=True)

# Format a data block mimicking the NYC Taxi schema structure
mock_record = {
    "vendor_id": "1",
    "tpep_pickup_datetime": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    "passenger_count": 2,
    "trip_distance": 2.5,
    "fare_amount": 12.50,
    "pickup_zip": "10001"
}

# Generate 3 individual JSON files to simulate incoming streams
for i in range(1, 4):
    mock_record["trip_distance"] += i  
    mock_record["fare_amount"] += (i * 4)
    
    with open(f"./local_staging/taxi_stream_{i}.json", "w") as f:
        json.dump(mock_record, f)
    print(f"Created local mock file: taxi_stream_{i}.json")
