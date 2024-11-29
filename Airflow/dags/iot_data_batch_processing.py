"""import os
from pyspark.sql import functions as F
from supabase import create_client
import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import ijson
import logging
from pyspark.sql import SparkSession
import findspark
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")
def read_json_lines(file_path):
    with open(file_path, 'rb') as file:
        data = ijson.items(file, 'item')
        extracted_data = []
        index = 0
        for vehicle_info in data:
            index+=1
            df = pd.json_normalize(vehicle_info, sep='_')
            d_flat = df.to_dict(orient='records')[0]
            extracted_data.append(d_flat)
            print(index,d_flat)


    return extracted_data

try:
    findspark.init()
    spark = SparkSession.builder.appName('ETL Pipeline').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logging.info('Spark session created successfully')
except Exception as e:
    logging.error("Couldn't create the spark session" , str(e))



@dag(
    dag_id="process_vehicle_data",
    start_date=datetime(year=2024, month=11, day=1, hour=9, minute=0),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={'execution_timeout': timedelta(minutes=190)}

)
def iot_data():
    @task()
    def extract_data_from_json_file():
        json_dir = './dags'
        data = []
        print("filesList 2", os.listdir(json_dir))

        for filename in os.listdir(json_dir):
            if filename.endswith('.json'):
                json_file_path = os.path.join(json_dir, filename)
                try:
                    print("Start Processing:", filename, "...")
                    file_data = read_json_lines(json_file_path)
                    data.extend(file_data)
                    print("End Processing:", filename, "...")
                except Exception as e:
                    print(f"An error occurred while processing file {filename}: {e}")

        return data

    @task()
    def extract_alerts_from_dataframe(data):
            try:
                df = spark.createDataFrame(data)
                print("1---")
                df.show(truncate=False)
                exploded_df = df.withColumn("alert", F.explode(F.col("alerts")))
                print("exploded_df----:")
                exploded_df.show(truncate=False)
                alerts_list = [row["alert"] for row in exploded_df.collect()]
                print("2---", len(alerts_list))
                return alerts_list
            except Exception as e:
                print(f"An error occurred while transforming the data", str(e))

    @task()
    def calculate_alerts_per_sevirity(data):
        df = spark.createDataFrame(data)
        count_df = df.groupBy("type", "severity").count()
        count_df.show()
        transformed_data = [
            {"type": row["type"], "severity": row["severity"], "count": row["count"]}
            for row in count_df.collect()
        ]
        return transformed_data

    @task()
    def load_data_into_supabase_table(transformed_data):
        try:
            SUPABASE_URL = os.getenv("SUPABASE_URL")
            SUPABASE_KEY = os.getenv("SUPABASE_KEY")
            # Initialize Supabase client
            supabase = create_client("https://snkgpdfnceyxtluftrrc.supabase.co",
                                     "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNua2dwZGZuY2V5eHRsdWZ0cnJjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzExNzIxMjYsImV4cCI6MjA0Njc0ODEyNn0.7HIvVYTAIdQMOQ5Wff_b1XOuJAMhzLlQLtJM3DQge5Y")
            response = supabase.table("AlertsPerSeverity").upsert(transformed_data).execute()

            if response.status_code == 201:
                print("Data loaded successfully into Supabase")
            else:
                print(f"Error loading data: {response.status_code}, {response.data}")
        except Exception as e:
            print(f"Error loading data: {e}")

    # Task dependencies
    raw_dataset = extract_data_from_json_file()
    transformed_dataset = extract_alerts_from_dataframe(raw_dataset)
    alerts = calculate_alerts_per_sevirity(transformed_dataset)
    load_data_into_supabase_table(alerts)


# Instantiate the DAG
iot_data()
"""