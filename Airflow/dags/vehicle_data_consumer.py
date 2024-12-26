import json
import time

from airflow.decorators import dag, task
from kafka import KafkaConsumer
from datetime import datetime, timedelta
import logging
import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves
# Kafka Consumer Function
def consume_data_from_kafka():
    consumer = KafkaConsumer(
        'sensor_data',
        bootstrap_servers=['broker:29092', 'localhost:9092'],
        group_id='vehicle_data_consumer',
        auto_offset_reset='earliest',  # Start reading from the earliest message
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for message in consumer:
        record = message.value
        print(f"Consumed record from Kafka: {record}")
        logging.info(f"Consumed record: {record}")
        time.sleep(1)

@dag(
    dag_id="vehicle_data_consumer",
    start_date=datetime(year=2024, month=12, day=24, hour=9, minute=0),
    schedule_interval="@daily",
    catchup=False,
    default_args={'execution_timeout': timedelta(minutes=190)}
)
def consume_vehicle_data():
    @task()
    def start():
        print("Consumer Job Started")

    @task()
    def consume_data():
        try:
            consume_data_from_kafka()
        except Exception as e:
            logging.error(f"Error while consuming data from Kafka: {e}")

    @task()
    def end():
        print("Consumer Job Ended")

    start() >> consume_data() >> end()

consume_vehicle_data()
