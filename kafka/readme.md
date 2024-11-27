This directory contains the **Docker Compose** configuration for setting up Apache Kafka and related services. Below is a summary of each service defined in the configuration:

### 1. **Zookeeper**
   - **Image**: `confluentinc/cp-zookeeper`
   - **Purpose**: Zookeeper is used for managing and coordinating Kafka brokers.
   - **Ports**: Exposes port `2181` for client communication.
   - **Environment Variables**:
     - `ZOOKEEPER_CLIENT_PORT`: Port for client connections.
     - `ZOOKEEPER_TICK_TIME`: Controls how often Zookeeper sends a tick to clients.
   - **Healthcheck**: Runs a command to ensure Zookeeper is responsive (`ruok` command).
   - **Network**: Connects to the `confluent` network.

### 2. **Broker**
   - **Image**: `confluentinc/cp-server`
   - **Purpose**: Kafka broker for handling messaging and data streaming.
   - **Depends on**: Zookeeper (ensures Zookeeper is healthy before starting).
   - **Ports**: Exposes ports `9092` (Kafka client communication) and `9101` (JMX metrics).
   - **Environment Variables**: 
     - Configures Kafka broker properties such as broker ID, listener settings, connection to Zookeeper, and JMX.
     - Includes integration with Confluent metrics reporting and schema registry.
   - **Healthcheck**: Checks if Kafka is running on port `9092` using `nc`.
   - **Network**: Connects to the `confluent` network.

### 3. **Schema Registry**
   - **Image**: `confluentinc/cp-schema-registry`
   - **Purpose**: Manages and validates Avro schemas (a way to define the structure of data to ensure data is consistent and can evolve over time) used with Kafka.
   - **Depends on**: Broker (ensures the Kafka broker is healthy before starting).
   - **Ports**: Exposes port `8081` for the schema registry API.
   - **Environment Variables**:
     - Connects to the Kafka broker and listens for schema requests.
   - **Healthcheck**: Uses `curl` to verify the schema registry is running and responsive.
   - **Network**: Connects to the `confluent` network.

### 4. **Control Center**
   - **Image**: `confluentinc/cp-enterprise-control-center`
   - **Purpose**: A management UI for monitoring and controlling Kafka clusters.
   - **Depends on**: Broker and Schema Registry (both need to be healthy before starting).
   - **Ports**: Exposes port `9021` for accessing the control center UI.
   - **Environment Variables**:
     - Configures connection to Kafka broker and schema registry, and sets various internal settings for monitoring and replication.
   - **Healthcheck**: Uses `curl` to check if the control center UI is healthy.
   - **Network**: Connects to the `confluent` network.

### Networks
   - **confluent**: Defines a custom network used by all services for internal communication.

### Summary
The configuration sets up a **Confluent platform** with:
- Zookeeper for Kafka coordination.
- Kafka broker for handling messaging.
- Schema Registry for managing data schemas.
- Control Center for monitoring the Kafka ecosystem.

Each service has health checks to ensure proper startup and availability. The services communicate over a shared network called `confluent`, and the setup exposes necessary ports for client access and monitoring tools.

**The Confluent Platform** is a set of tools built around Apache Kafka that helps organizations manage and process real-time data streams. It makes it easier to collect, store, and analyze data as it flows through systems, allowing businesses to react quickly to changes.