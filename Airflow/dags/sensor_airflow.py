import json
import os

from airflow.decorators import dag, task
import ijson
from decimal import Decimal
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
import logging
import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

# Function to read JSON data from a file
def read_json_lines(file_path):
    with open(file_path, 'rb') as file:
        data = ijson.items(file, 'item')
        for record in data:
            yield record


def convert_decimals(record):
    """Convert Decimal values to floats in the record."""
    for key, value in record.items():
        if isinstance(value, Decimal):
            record[key] = float(value)  # Convert Decimal to float
        elif isinstance(value, dict):
            convert_decimals(value)  # Recursively handle nested dictionaries
    return record


PATH = './traffic_data_1.json'


# Airflow DAG definition
@dag(
    dag_id="vehicle_data_streaming",
    start_date=datetime(year=2024, month=11, day=1, hour=9, minute=0),
    schedule_interval="@daily",
    catchup=False,
    default_args={'execution_timeout': timedelta(minutes=190)}
)
def iot_data():
    @task()
    def start():
        print("Jobs Started")

        current_location = os.getcwd()
        print(f"Current Location: {current_location}")

        # List all folders in the current directory
        folders = [f for f in os.listdir(current_location) if os.path.isdir(os.path.join(current_location, f))]
        print("Folders in the Current Path:")
        for folder in folders:
            print(f" - {folder}")
    @task()
    def stream_data_from_json(json_file_path):
        print("Producer is about to be created ! ")
        producer = KafkaProducer(bootstrap_servers=['broker:29092', 'localhost:9092'], max_request_size=104857600,max_block_ms=5000)
        print("Producer created ! ")
        try:
            # Read data from the JSON file
            index = 0
            # Read each JSON entry and send to Kafka
            for record in read_json_lines(json_file_path):
                # Convert any Decimal values in the record to float
                record = convert_decimals(record)

                # Send the record to Kafka
                producer.send('sensor_data', json.dumps({"record":"record"}).encode('utf-8'))
                print(f"Sent record to Kafka: {record}")
                time.sleep(1)  # Simulate a small delay between records

                index += 1
                if index >= 100:  # Process a maximum of 100 records for this run (optional)
                    break
        except Exception as e:
            logging.error(f'An error occurred while processing the JSON data: {e}')
        finally:
            producer.flush()  # Ensure all messages are sent to Kafka before finishing

    @task()
    def end():
        print("Jobs Ended")

    # Task dependencies: ensure tasks run in the correct order
    start() >> stream_data_from_json(PATH) >> end()


# Instantiate the DAG
iot_data()
