# Data Ingestion & Streaming Pipeline

This project implements a robust data ingestion and streaming pipeline designed to process real-time e-commerce events from Kafka, enrich them with metadata, and land them into a Google Cloud-based data warehouse for analytical processing.

## 1. Project Overview

The pipeline consumes event data (specifically product views) from a Kafka cluster, processes and cleans the data using PySpark Streaming, and performs real-time enrichment. The architecture follows a multi-stage approach where data is landed in BigQuery and archived to Google Cloud Storage (GCS) as a "Bronze" layer. Once the data is in BigQuery, **dbt** (Data Build Tool) is utilized for downstream data cleaning, transformation, and modeling, moving data through Bronze, Silver, and Gold layers for final analysis.

## 2. Key Features

* **Real-time Streaming**: Consumes JSON-formatted event data from the `product_view` Kafka topic.
* **User-Agent Enrichment**: Utilizes custom Spark UDFs to parse raw user-agent strings into structured data (browser, OS, device family, brand, and model).
* **Automated Data Cleaning**: Flattens nested product options and removes null or empty values before ingestion.
* **Hybrid Storage Strategy**:
* **BigQuery**: Real-time insertion into `summary`, `products`, and `user_agent` tables.
* **GCS Archive**: Exports temporary batch data to Parquet format in GCS (`gs://big_data_project_final/bronze/summary/`) for long-term storage and Bronze-layer persistence.


* **Parallel Web Crawling**: A dedicated enrichment script crawls the Glamira website to retrieve product names for IDs captured in the stream.
* **Batch IP Geolocation**: A utility for mapping IP addresses to geographical locations using a local IP2Location database.

## 3. Tech Stack

* **Streaming Engine**: PySpark (Spark SQL Kafka connector v0-10).
* **Message Broker**: Apache Kafka.
* **Data Warehouse**: Google BigQuery.
* **Cloud Storage**: Google Cloud Storage (GCS).
* **Transformation Layer**: dbt (used for Bronze → Silver → Gold modeling).
* **Programming Language**: Python 3.12.
* **Key Libraries**: `pandas`, `beautifulsoup4`, `requests`, `google-cloud-bigquery`, `user-agents`.

## 4. Project Architecture / Folder Structure

The ingestion component is organized as follows:

* `spark_streaming.py`: The core streaming application handling Kafka consumption, data cleaning, enrichment, and BQ/GCS ingestion.
* `schemas.py`: Defines the Spark `StructType` schema (`event_schema`) for the incoming Kafka JSON payloads, including nested cart and product details.
* `browser/`: Contains logic for User-Agent parsing and Spark UDF definitions.
* `crawl_product_name.py`: Independent script that fetches uncrawled product IDs from BigQuery and updates them with names crawled via HTTP.
* `get_iplocation/`: Utility for batch processing IP addresses into location metadata.
* `key.json`: Service account credentials for Google Cloud authentication.
* **Transformation (dbt)**: A separate layer (not included in this source directory) responsible for transforming the ingested "Bronze" data in BigQuery into "Silver" and "Gold" analytical models.

## 5. Setup & Installation

1. **Prerequisites**:
* Python 3.12+
* Apache Spark 3.5.0 cluster with Kafka connectors.
* A Google Cloud Project with BigQuery and GCS enabled.


2. **Install Dependencies**:
```bash
pip install -r requirements.txt

```


*(Dependencies include: `pandas`, `google-cloud-bigquery`, `user-agents`, etc.)*
3. **Credentials**:
* Ensure `key.json` is placed in the project root to allow BigQuery and Storage client authentication.


4. **Kafka Configuration**:
* The system expects a Kafka broker at the specified IP (46.202.167.130) using SASL_PLAINTEXT authentication.



## 6. How to Run

### Start the Streaming Pipeline

Execute the PySpark application to begin ingesting Kafka events:

```bash
python spark_streaming.py

```

### Run Product Name Crawler

To enrich the ingested product data with names:

```bash
python crawl_product_name.py

```

### Run IP Geolocation Utility

To process a list of IPs from `ips.txt`:

```bash
python get_iplocation/get_iplocation.py

```

## 7. Configuration

The following configurations are hardcoded or managed via environment variables in `spark_streaming.py` and `crawl_product_name.py`:

* **GCP Project ID**: `bigdataproject-474006`.
* **BigQuery Dataset**: `Glamira`.
* **Kafka Topic**: `product_view`.
* **GCS Bucket**: `big_data_project_final`.
* **Checkpointing**: Spark checkpoints are stored in `/tmp/spark_checkpoints/`.

## 8. Usage Examples

### Manual Export Trigger

The `spark_streaming.py` application monitors a local trigger file. To manually force an export of the temporary BigQuery summary data to GCS:

```bash
touch /tmp/export_trigger.txt

```

The watcher thread will detect the file, execute the `EXPORT DATA` SQL command, and delete the temporary table.

## 9. Notes / Limitations

* **dbt Models**: This repository focuses strictly on the ingestion layer. dbt transformation models, tests, and YAML configurations are maintained separately.
* **Service Account**: The provided code relies on a local service account file (`key.json`). In production, Google Application Default Credentials (ADC) or Secret Manager are recommended.
* **Crawl Retries**: The product crawler is limited to 3 retries per URL to avoid IP blocking.

## 10. Future Improvements

* Integrate the IP geolocation logic directly into the Spark Streaming UDFs for real-time location enrichment.
* Implement a more robust configuration management system (e.g., `.env` or YAML) to replace hardcoded project IDs and Kafka IPs.
* Automate the trigger of the `crawl_product_name.py` script via a workflow orchestrator like Apache Airflow.
