from airflow.decorators import dag, task
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
vehicle_metrics_path = "/tmp/vehicle_metrics"
traffic_analysis_path = "/tmp/traffic_analysis"
vehicule_schema = """
                    vehicle_id STRING,
                    owner STRUCT<name STRING, license_number STRING, contact_info STRUCT<phone STRING, email STRING>>,
                    speed_kmph FLOAT,
                    road STRUCT<street STRING, district STRING, city STRING>,
                    timestamp STRING,
                    vehicle_size STRUCT<length_meters FLOAT, width_meters FLOAT, height_meters FLOAT>,
                    vehicle_type STRING,
                    vehicle_classification STRING,
                    coordinates STRUCT<latitude FLOAT, longitude FLOAT>,
                    engine_status STRUCT<is_running BOOLEAN, rpm INT, oil_pressure STRING>,
                    fuel_level_percentage INT,
                    passenger_count INT,
                    internal_temperature_celsius FLOAT,
                    weather_condition STRUCT<temperature_celsius FLOAT, humidity_percentage FLOAT, condition STRING>,
                    estimated_time_of_arrival STRUCT<destination STRUCT<street STRING, district STRING, city STRING>, eta STRING>,
                    traffic_status STRUCT<congestion_level STRING, estimated_delay_minutes INT>,
                    alerts ARRAY<STRUCT<type STRING, description STRING, severity STRING, timestamp STRING>>
                """


def create_spark_session():
    return (SparkSession.builder
            .appName('Vehicle ETL Pipeline')
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
            .getOrCreate())


@dag(
    dag_id="vehicule_data_processing",
    start_date=datetime(year=2024, month=12, day=24, hour=9, minute=0),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1
)

def vehicle_etl_workflow():
    @task()
    def extract_raw_data():
        """Extract data from Kafka and save as raw parquet"""
        spark = create_spark_session()
        print("Spark session created !")

        try:
            # Read from Kafka
            raw_df = spark.read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "broker:29092") \
                .option("subscribe", "sensor_data") \
                .option("startingOffsets", "earliest") \
                .option("endingOffsets", "latest") \
                .load()
            print("spark df created !")
            raw_df.show()

            # Save raw data
            output_path = "/tmp/raw_vehicle_data"
            raw_df.write.mode("overwrite").parquet(output_path)
            return output_path
        finally:
            spark.stop()

    @task()
    def clean_and_validate(raw_data_path):
        """Clean data and perform validations"""
        spark = create_spark_session()
        print("Spark session created !")
        try:
            # Read raw data
            df = spark.read.parquet(raw_data_path)
            print("spark df created !")
            df.show()
            # Parse JSON and validate schema
            parsed_df = df.selectExpr("CAST(value AS STRING) as json_data") \
                .select(F.from_json(F.col("json_data"), vehicule_schema).alias("data")) \
                .select("data.*")

            # Clean and validate
            cleaned_df = parsed_df \
                .dropDuplicates(["vehicle_id", "timestamp"]) \
                .filter(F.col("speed_kmph").isNotNull()) \
                .filter(F.col("timestamp").isNotNull()) \
                .withColumn("processing_date", F.current_date())
            cleaned_df.show()
            # Validate data quality
            invalid_records = cleaned_df.filter(
                (F.col("speed_kmph").isNotNull() & (F.col("speed_kmph") < 0)) |
                (F.col("fuel_level_percentage").isNotNull() & (F.col("fuel_level_percentage") < 0)) |
                (F.col("fuel_level_percentage") > 100) |
                (F.col("passenger_count").isNotNull() & (F.col("passenger_count") < 0))
            )

            # Save validation results
            validation_path = "/tmp/validation_results"
            invalid_records.write.mode("overwrite").parquet(validation_path)

            # Save cleaned data
            output_path = "/tmp/cleaned_vehicle_data"
            cleaned_df.write.mode("overwrite").parquet(output_path)

            return output_path
        except Exception as e:
            print(f"Error clean_and_validate: {str(e)}")

    @task()
    def transform_vehicle_metrics(cleaned_data_path):
        """Transform and calculate vehicle-specific metrics"""
        spark = create_spark_session()
        print("Spark session created !")
        try:
            df = spark.read.parquet(cleaned_data_path)
            df.show()

            # Calculate vehicle metrics
            metrics_df = df \
                .withColumn("trip_duration",
                            F.unix_timestamp(F.col("estimated_time_of_arrival.eta")) -
                            F.unix_timestamp(F.col("timestamp"))
                            ) \
                .groupBy("vehicle_type") \
                .agg(
                F.round(F.avg("speed_kmph"),2).alias("avg_speed"),
                F.round(F.avg("trip_duration")).alias("avg_trip_duration"),
                F.count("*").alias("total_trips")
            )
            metrics_df.show()

            output_path = "/tmp/vehicle_metrics"
            metrics_df.write.mode("overwrite").parquet(output_path)
            return output_path
        except Exception as e:
            print(f"Error transform_vehicle_metrics: {str(e)}")

    @task()
    def transform_traffic_analysis(cleaned_data_path):
        """Transform and analyze traffic patterns"""
        spark = create_spark_session()
        print("Spark session created !")
        try:
            df = spark.read.parquet(cleaned_data_path)

            # Analyze traffic patterns
            traffic_df = df \
                .groupBy(
                F.date_trunc("hour", F.col("timestamp")).alias("hour"),
                "road.district",
                "road.city"
            ) \
                .agg(
                F.count("*").alias("vehicle_count"),
                F.round(F.avg("speed_kmph"),2).alias("avg_speed"),
            )

            traffic_df.show()
            output_path = "/tmp/traffic_analysis"
            traffic_df.write.mode("overwrite").parquet(output_path)
            return output_path
        except Exception as e:
            print(f"Error transform_traffic_analysis: {str(e)}")

    @task()
    def transform_vehicule_alerts(cleaned_data_path):
        spark = create_spark_session()
        print("Spark session created !")
        try:
            # Read the saved batch data
            df = spark.read.parquet(cleaned_data_path)

            # Explode alerts array and process
            alerts_df = df.select(
                "vehicle_id",
                "timestamp",
                F.explode("alerts").alias("alert")
            ).select(
                "vehicle_id",
                "timestamp",
                "alert.type",
                "alert.severity",
                "alert.description"
            )

            # Aggregate alerts by type and severity
            summary_df = alerts_df.groupBy("type", "severity") \
                .agg(
                F.count("*").alias("alert_count"),
                F.collect_list("description").alias("descriptions")
            )
            summary_df.show()
            # Save results
            output_path = "/tmp/alerts_summary"
            summary_df.write \
                .mode("overwrite") \
                .parquet(output_path)

            return output_path
        finally:
            spark.stop()
    @task()
    def load_to_data_warehouse(vehicle_metrics_path,traffic_analysis_path,alerts_analaysis_path):
        """Load processed data to data warehouse"""
        spark = create_spark_session()
        print("Spark session created !")
        try:

            metrics_df = spark.read.parquet(vehicle_metrics_path)
            traffic_df = spark.read.parquet(traffic_analysis_path)
            alerts_df = spark.read.parquet(alerts_analaysis_path)

            metrics_df.show()
            traffic_df.show()
            alerts_df.show()
            print('Load data to ELK !')
        except Exception as e:
            print(f"Error loading: {str(e)}")

    # Define workflow
    raw_data_path = extract_raw_data()
    cleaned_data_path = clean_and_validate(raw_data_path)
    vehicle_metrics_path = transform_vehicle_metrics(cleaned_data_path)
    traffic_analysis_path = transform_traffic_analysis(cleaned_data_path)
    alerts_analaysis_path = transform_vehicule_alerts(cleaned_data_path)
    load_to_data_warehouse(vehicle_metrics_path, traffic_analysis_path, alerts_analaysis_path)


dag = vehicle_etl_workflow()