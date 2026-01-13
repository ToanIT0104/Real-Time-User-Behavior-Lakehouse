# Real-Time Lakehouse Ingestion Pipeline

This project implements a modern **Lakehouse architecture** designed for real-time ingestion, enrichment, and processing of e-commerce clickstream data. The pipeline leverages Spark Structured Streaming to consume events from Kafka, enriches data on the fly (User-Agent parsing, product metadata crawling, and geolocation), and manages the data lifecycle from raw ingestion to analytical readiness in Google BigQuery and Google Cloud Storage.

## 1. Project Overview

The platform follows a Lakehouse pattern to bridge the gap between the flexibility of a data lake and the performance of a data warehouse.

**End-to-End Data Flow:**

1. **Ingestion:** Captured real-time events from a multi-node Kafka cluster.
2. **Stream Processing:** Spark Structured Streaming consumes raw JSON events, enforces schema, and enriches User-Agent metadata using custom UDFs.
3. **Storage (Bronze):** Raw and semi-processed data is stored as the Bronze layer. Staging occurs in BigQuery temporary tables before being exported to Google Cloud Storage (GCS) in Parquet format.
4. **Transformation:** dbt (Data Build Tool) is responsible for data cleaning, transformation, and modeling through Silver and Gold layers.
5. **Analytics:** Final analytical tables are materialized in Google BigQuery for business intelligence.

## 2. Key Features

* **Real-Time Streaming:** Robust Kafka-to-Spark integration with SASL authentication.
* **User-Agent Enrichment:** Custom PySpark UDFs to extract Browser, OS, and Device specifications (Brand, Model, Family) using the `user-agents` library.
* **Automated Product Crawling:** A multi-threaded web scraper (using BeautifulSoup) that retrieves missing product metadata from the Glamira website and updates the BigQuery catalog.
* **Staging & Export Logic:** Automated "foreachBatch" processing that inserts records into BigQuery and triggers exports to GCS as Parquet files.
* **IP Geolocation:** A batch utility for mapping IP addresses to geographic coordinates (Country, Region, City) using a local IP2Location database.

## 3. Tech Stack

* **Ingestion:** Apache Kafka (SASL_PLAINTEXT).
* **Stream Processing:** Apache Spark 3.5 (Structured Streaming), PySpark.
* **Storage & Lakehouse:** Google Cloud Storage (GCS), Apache Parquet.
* **Data Warehouse:** Google BigQuery.
* **Transformation:** dbt (modeling logic managed externally).
* **Libraries:** `user-agents` (UA parsing), `BeautifulSoup4` (Scraping), `pandas` (Geolocation), `venv-pack`.

## 4. Lakehouse Architecture

* **Bronze Layer:** Contains raw and semi-processed events. Data is stored in GCS as Parquet files, serving as the immutable source of truth.
* **Silver Layer:** Managed by dbt; involves data cleaning, filtering, and joining streaming events with enriched product and user metadata.
* **Gold Layer:** Managed by dbt; contains high-level analytical models and aggregated tables optimized for BI tools in BigQuery.

## 5. Project Structure

```text
IngestionData/
├── browser/
│   └── browser.py           # User-Agent parsing UDFs and logic
├── get_iplocation/
│   └── get_iplocation.py    # IP Geolocation batch processing utility
├── crawl_product_name.py    # Multi-threaded web scraper for product metadata
├── spark_streaming.py       # Main entry point for Kafka-to-BigQuery streaming
├── schemas.py               # StructType definitions for JSON parsing
├── requirements.txt         # Python dependencies
└── key.json                 # GCP Service Account credentials

```

## 6. Setup & Installation

1. **Prerequisites:** Ensure you have Docker, Apache Spark, and a Google Cloud Project with BigQuery and GCS enabled.
2. **Environment:** Place your Google Cloud service account key (`key.json`) in the `IngestionData` directory.
3. **Kafka Access:** The pipeline is configured to connect to the Kafka cluster at `46.202.167.130:9094`.

## 7. How to Run the Project

The entire ingestion and streaming pipeline is executed using a single Docker command, which handles environment preparation, dependency installation, and Spark job submission inside a containerized Spark runtime.

### 1. Start the Lakehouse Ingestion Pipeline

Run the following command from the project root to package the source code, start a Spark container, and launch the streaming job:

```powershell
Remove-Item -Path .\BigDataProject\BigDataProject.zip -Force -ErrorAction SilentlyContinue
Compress-Archive -Path .\BigDataProject\* -DestinationPath .\BigDataProject\BigDataProject.zip -Force

docker container stop test-spark
docker container rm test-spark

docker run -ti --name test-spark `
    --network=streaming-network `
    -p 4040:4040 `
    -v ${PWD}:/spark `
    -v spark_lib:/opt/bitnami/spark/.ivy2 `
    -v spark_data:/data `
    -e PYSPARK_DRIVER_PYTHON=python `
    -e PYSPARK_PYTHON=./environment/bin/python `
    -e GOOGLE_APPLICATION_CREDENTIALS=/spark/BigDataProject/key.json `
    unigap/spark:3.5 bash -c "
        python -m venv pyspark_venv && \
        source pyspark_venv/bin/activate && \
        pip install -r /spark/BigDataProject/requirements.txt && \
        venv-pack -o pyspark_venv.tar.gz && \
        spark-submit \
            --repositories https://repo1.maven.org/maven2/,https://storage.googleapis.com/spark-lib/bigquery \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
            --archives pyspark_venv.tar.gz#environment \
            --py-files /spark/BigDataProject/BigDataProject.zip,/spark/BigDataProject/browser.zip \
            /spark/BigDataProject/spark_streaming.py
    "

```

This command performs the following steps:

* Packages the ingestion source code into a ZIP archive.
* Starts a Spark container connected to the Kafka network.
* Creates and packages a Python virtual environment.
* Submits the Spark Structured Streaming job.
* Begins consuming Kafka events and ingesting data into BigQuery.

**Spark UI is exposed at:** 👉 `http://localhost:4040`

### 2. Trigger Export to GCS (Bronze Layer)

To export the current staging data from BigQuery to Google Cloud Storage (Bronze layer), run:

```bash
docker exec -it test-spark touch /tmp/export_trigger.txt

```

The streaming application monitors this trigger file and will:

* Execute the BigQuery `EXPORT DATA` operation.
* Write Parquet files to the GCS Bronze layer.
* Clean up temporary staging tables.

## 8. Configuration

Important variables found in the source code:

* **GCP Project ID:** `bigdataproject-474006`
* **BigQuery Dataset:** `Glamira`
* **Kafka Bootstrap Servers:** `46.202.167.130:9094, 46.202.167.130:9194, 46.202.167.130:9294`
* **Kafka Topic:** `product_view`
* **GCS Bucket:** `gs://big_data_project_final/`

## 9. Notes / Limitations

* **dbt Models:** This repository contains only the ingestion and streaming code. dbt models for Silver and Gold layers are not included in this source.
* **IP Database:** The `get_iplocation.py` script requires an external CSV (`IP2LOCATION-LITE-DB5.CSV`) which is not included.
* **Hardcoded Paths:** The `crawl_product_name.py` script contains a hardcoded local path for the GCP key that may need adjustment for Linux/Docker environments.

## 10. Future Improvements

* Implement workflow orchestration (e.g., Apache Airflow) to automate the product scraper and dbt transformation runs.
* Add unit tests for Spark UDFs and schema validation logic.
* Parameterize hardcoded configurations into a central `.env` or YAML file.

Data Visualization & Insights
The analytical output of the Lakehouse platform is materialized into Gold-layer tables within BigQuery and visualized through Looker Studio to support data-driven decision-making.

Executive Sales Overview
<img width="1181" height="490" alt="Image" src="https://github.com/user-attachments/assets/03a9c954-69ef-4d0b-b9ab-e921bc134352" />
This dashboard provides a high-level summary of core business KPIs, including revenue performance, order volume, and customer acquisition metrics. It enables stakeholders to monitor global revenue distribution by country and track daily financial trends at a glance.

Product Performance & Material Analysis
<img width="1107" height="762" alt="Image" src="https://github.com/user-attachments/assets/d3841d8a-1951-4fe0-8896-20121babbf3d" />
Focused on inventory and catalog intelligence, this dashboard analyzes sales velocity and revenue generation across top products. It specifically identifies customer preferences for various materials and gemstones, providing actionable insights for product development and marketing strategies.

Geographic & Technical Audience Breakdown
<img width="1201" height="805" alt="Image" src="https://github.com/user-attachments/assets/728c637d-1bfb-434b-ba95-d91ba9b332b8" />

<img width="1150" height="813" alt="Image" src="https://github.com/user-attachments/assets/8d149dfb-782e-473a-b2c7-fe78c686f51a" />
This visualization maps global revenue across specific regions and cities to pinpoint high-growth markets. Additionally, it correlates transaction data with technical metadata—such as device types and web browsers—to help optimize the front-end user experience and target platform-specific optimizations.
