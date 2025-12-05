🚀 Real-Time Clickstream Analytics Pipeline
A comprehensive Data Engineering project demonstrating the implementation of a Real-Time Data Lakehouse (Medallion Architecture) using Apache Kafka, Apache Spark Structured Streaming, and Apache Hudi for high-volume clickstream event processing and real-time dashboarding.

🎯 Project Goals & Milestones
The primary goal is to build an end-to-end, fault-tolerant pipeline that processes raw website events into finalized, enriched metrics for live analysis.

Milestone	Status	Description
1. Data Ingestion	✅ Completed	Designed a concurrent Python simulator and established reliable ingestion into Kafka.
2. Bronze Layer	✅ Completed	Ingested raw Kafka data directly into the Hudi Data Lake for immutable storage.
3. Silver Layer (In Progress)	🚧 In Progress	Parsing, GeoIP enrichment, and initial saving of cleansed events back to Hudi.
4. Aggregation & Gold Layer	⬜ Pending	Sessionization (windowing), final aggregation, and loading finalized metrics into PostgreSQL.
5. Live Dashboard	⬜ Pending	Visualization of Gold Layer metrics using Grafana for real-time reporting.

🏗️ Architecture Overview: The Medallion Approach
This project follows the Medallion Architecture (Bronze, Silver, Gold), which ensures data quality, governance, and lineage throughout the pipeline.

1. Bronze Layer (Raw Storage)
Input: Real-time stream from Kafka.

Action: Ingests raw event JSON into Hudi with minimal processing (adding ingestion time and offset). Data is immutable.

Storage: Hudi Data Lake (/data/bronze/clickstream/)

2. Silver Layer (Cleanse & Enrich)
Input: Continuous stream from the Bronze Hudi table.

Action: Parses raw JSON, performs GeoIP enrichment (adding city, country), and cleanses data (type casting, validation).

Storage: Hudi Data Lake (/data/silver/enriched_events/)

3. Gold Layer (Aggregated & Ready-to-Serve)
Input: Continuous stream from the Silver layer (and a dedicated Kafka topic).

Action: Performs Session Windowing and calculates final business metrics (e.g., Session Duration, Conversion Rate). Loads metrics into the PostgreSQL Data Warehouse.

Storage: PostgreSQL Database (for dashboards).

🛠️ Technology Stack (Big Data & Real-Time)

1) Simulation--Python (threading, kafka-python)--Simulates high-volume, concurrent user clickstream events.
  
2) Message Queue--Apache Kafka--Acts as the reliable, high-throughput buffer for real-time events.
   
3) Processing Engine--Apache--Spark Structured Streaming for scalable, fault-tolerant ETL operations (Bronze, Silver, Gold).
   
4) Data Lakehouse--Apache Hudi--Provides ACID transactions, schema evolution, and UPSERT capabilities for the Data Lake layers.
   
5) Data Warehouse--PostgreSQL--Final destination for Gold Layer metrics, optimized for BI reporting.
    
6) Orchestration--Docker--Docker ComposeUsed to manage the local environment for Kafka, Spark, and PostgreSQL services.

✅ Completed Milestones
Milestone 1 & 2: Data Ingestion & Bronze Layer
This phase established the reliable flow of raw data from the simulator into the Data Lake.

1. Concurrent Data Generation (producer.py)
Simulates multiple users simultaneously using Python threading.

Generates well-structured JSON events with essential keys: user_id, session_id, timestamp, and ip_address.

Uses a globally initialized KafkaProducer with graceful shutdown (flush()) for high throughput.

2. Bronze Layer Ingestion (bronze_ingestion.py)
Reads the raw JSON stream from the clickstream_events Kafka topic.

Writes the stream to the Hudi Data Lake using Delta Lake format for reliability.

Output: Immutable Parquet files and transactional logs in /data/bronze/clickstream/.

Current Architecture State
The current pipeline successfully moves raw, high-volume data from the source into the first layer of the Data Lakehouse, ready for the Silver layer to pick up.
