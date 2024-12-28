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
    if isinstance(record, dict):
        return {key: convert_decimals(value) for key, value in record.items()}
    elif isinstance(record, list):
        return [convert_decimals(item) for item in record]
    elif isinstance(record, Decimal):
        return float(record)
    else:
        return record


PATH = './data/traffic_data_0.json'


# Airflow DAG definition
@dag(
    dag_id="vehicle_data_streamer",
    start_date=datetime(year=2024, month=11, day=1, hour=9, minute=0),
    schedule_interval="@daily",
    catchup=False,
    default_args={'execution_timeout': timedelta(minutes=190)}
)
def iot_data():
    @task()
    def start():
        print("Jobs Started")
    @task()
    def stream_data_from_json(json_file_path):
        try:
            producer = KafkaProducer(bootstrap_servers=['broker:29092', 'localhost:9092'],
                                     max_request_size=104857600,
                                     max_block_ms=5000,
                                     value_serializer=lambda v: json.dumps(v).encode('utf-8')
                                     )
            print("Producer created ! ")

            # Read data from the JSON file
            index = 0
            # Read each JSON entry and send to Kafka
            for record in read_json_lines(json_file_path):
                # Convert any Decimal values in the record to float
                record = convert_decimals(record)
                producer.send('sensor_data', record)
                print(f"Sent record to Kafka: {index}-{record}")
                time.sleep(1)

                index += 1
        except Exception as e:
            logging.error(f'An error occurred while processing the JSON data: {str(e)}')
        finally:
            producer.flush()  # Ensure all messages are sent to Kafka before finishing

    @task()
    def end():
        print("Jobs Ended")

    # Task dependencies: ensure tasks run in the correct order
    start() >> stream_data_from_json(PATH) >> end()


# Instantiate the DAG
iot_data()
