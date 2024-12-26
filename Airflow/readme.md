# Apache Airflow Pipelines for Kafka to Elasticsearch

This project contains two Apache Airflow pipelines that work together to process data from a **JSON file**, send it to **Kafka**, consume the data, and save it into **Elasticsearch** for analysis and search purposes. Below is an overview of the two pipelines.

## Table of Contents

1. [Pipeline 1: JSON to Kafka Producer](#pipeline-1-json-to-kafka-producer)
2. [Pipeline 2: Kafka Consumer to Elasticsearch](#pipeline-2-kafka-consumer-to-elasticsearch)
3. [Airflow Setup](#airflow-setup)
4. [Dependencies](#dependencies)

---

## Pipeline 1: JSON to Kafka Producer

### Overview:
This pipeline reads data from a **JSON file** and sends it to a **Kafka topic** for further processing. It is designed to extract structured data from the JSON file and publish the data to Kafka.

### Steps:
1. **Read JSON File**: The pipeline reads data from a JSON file containing structured information.
2. **Kafka Producer**: The data is then sent to a Kafka topic (e.g., `json_data_topic`) using the Kafka producer API. 
3. **Error Handling**: The pipeline includes error handling mechanisms to manage any issues with reading the file or sending data to Kafka.

### Key Components:
- **KafkaProducerOperator**: To send the data to Kafka.


---

## Pipeline 2: Kafka Consumer to Elasticsearch

### Overview:
This pipeline is responsible for consuming data from the **Kafka topic** and saving it to **Elasticsearch**. The data can then be used for real-time analytics or as part of a search system.

### Steps:
1. **Kafka Consumer**: The pipeline listens to the Kafka topic.
2. **Elasticsearch Indexing**: Once data is received from Kafka, it is parsed and indexed into Elasticsearch for efficient querying and analysis.
3. **Error Handling**: Similar to Pipeline 1, this pipeline also handles potential errors such as issues with consuming messages from Kafka or indexing to Elasticsearch.


### Key Components:
- **KafkaConsumerOperator**: To consume messages from the Kafka topic.
- **ElasticsearchOperator**: To index the data into Elasticsearch.
- **JsonParser**: To parse and format the data before storing it in Elasticsearch.

---



## Docker Setup

The provided `Dockerfile` configures a custom Airflow image, which includes the necessary dependencies to run the pipelines. Key points in the Dockerfile:

1. **Base Image**: Uses `apache/airflow:2.9.3-python3.11`.
2. **Dependencies**: Installs required Python packages using `requirements.txt`.
3. **Java Setup**: Installs Java to support Kafka integration.
4. **Airflow Configuration**: Customizes Airflow setup for optimal usage in the pipeline environment.

---

## Airflow Docker Compose Configuration
**Docker Services**:
   - **postgres**: A PostgreSQL service used as the Airflow database. It has a health check and persistent storage (via a volume).
   - **airflow-webserver**: Runs the Airflow web server, exposing port 8090. It includes health checks and depends on the initialization of Airflow.
   - **airflow-scheduler**: Runs the scheduler service, responsible for executing tasks according to the DAGs.
   - **airflow-triggerer**: Executes trigger-related jobs.
   - **airflow-init**: Initializes the Airflow environment, checking system resources (memory, CPU, disk space) and setting appropriate permissions.
   - **airflow-cli**: Provides a CLI interface for interacting with the Airflow environment.

**Volumes**: 
   - Various directories (e.g., `dags`, `logs`, `plugins`) are mounted as volumes to store persistent data and logs, ensuring that data is not lost between container restarts.
   - A volume for the PostgreSQL database is also defined to persist data.

**Dependencies**: Services like the web server, scheduler, and triggerer depend on the initialization process and the availability of the PostgreSQL service.

---

## Running the Pipelines

To run the pipelines, follow these steps:

1. **Build the Docker Image**:
   ```bash
   docker-compose build
   ```

2. **Start Airflow and Required Services**:
   ```bash
   docker-compose up
   ```

3. **Access Airflow Web UI**: Open your browser and go to [http://localhost:8090](http://localhost:8090) to monitor and trigger the pipelines.

---

For any errors in the logs, we can check the Docker Compose logs for detailed error messages:

```bash
docker-compose logs
```