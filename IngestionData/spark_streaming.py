import logging
import os
import hashlib
import time
import signal
import sys
import re
from google.cloud import storage
import threading

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import from_json
from google.cloud import bigquery
from schemas import event_schema
from browser.browser import (
    browser_family_udf, browser_version_udf, os_family_udf, os_version_udf,
    device_family_udf, device_brand_udf, device_model_udf,
    is_mobile_udf, is_tablet_udf, is_touch_capable_udf, is_pc_udf, is_bot_udf
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("SparkStreamingApp")

PROJECT_ID = "bigdataproject-474006"
DATASET = "Glamira"
SUMMARY_TABLE = f"{PROJECT_ID}.{DATASET}.summary"
PRODUCTS_TABLE = f"{PROJECT_ID}.{DATASET}.products"
USER_AGENT_TABLE = f"{PROJECT_ID}.{DATASET}.user_agent"

RUN_ID = int(time.time())
TEMP_SUMMARY_TABLE = f"{PROJECT_ID}.{DATASET}.summary_tmp_{RUN_ID}"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./key.json"
bq_client = bigquery.Client.from_service_account_json("/spark/BigDataProject/key.json")

source_kafka = {
    "bootstrap_servers": "46.202.167.130:9094,46.202.167.130:9194,46.202.167.130:9294",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanisms": "PLAIN",
    "sasl_username": "kafka",
    "sasl_password": "UnigapKafka@2024",
    "group_id": "minhtoan-consumer-group",
    "auto_offset_reset": "latest",
    "topic": "product_view"
}

kafka_options = {
    "kafka.bootstrap.servers": source_kafka["bootstrap_servers"],
    "subscribe": source_kafka["topic"],
    "startingOffsets": source_kafka.get("auto_offset_reset", "latest")
}
if source_kafka["security_protocol"].startswith("SASL"):
    kafka_options.update({
        "kafka.security.protocol": source_kafka["security_protocol"],
        "kafka.sasl.mechanism": source_kafka.get("sasl_mechanisms", "PLAIN"),
        "kafka.sasl.jaas.config": f"org.apache.kafka.common.security.plain.PlainLoginModule required "
                                  f"username='{source_kafka['sasl_username']}' "
                                  f"password='{source_kafka['sasl_password']}';"
    })

spark = SparkSession.builder \
    .appName("SparkStreamingApp") \
    .master("spark://spark:7077") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
log.info(f"Spark Session started, using temporary table: {TEMP_SUMMARY_TABLE}")

df = (
    spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .option("failOnDataLoss", "false")
        .load()
)

value_df = df.selectExpr("CAST(value AS STRING) AS json_str")
parsed_df = value_df.select(from_json("json_str", event_schema).alias("data")).select("data.*")

ua_df = parsed_df \
    .withColumn("browser_family", browser_family_udf("user_agent")) \
    .withColumn("browser_version", browser_version_udf("user_agent")) \
    .withColumn("os_family", os_family_udf("user_agent")) \
    .withColumn("os_version", os_version_udf("user_agent")) \
    .withColumn("device_family", device_family_udf("user_agent")) \
    .withColumn("device_brand", device_brand_udf("user_agent")) \
    .withColumn("device_model", device_model_udf("user_agent")) \
    .withColumn("is_mobile", is_mobile_udf("user_agent")) \
    .withColumn("is_tablet", is_tablet_udf("user_agent")) \
    .withColumn("is_touch_capable", is_touch_capable_udf("user_agent")) \
    .withColumn("is_pc", is_pc_udf("user_agent")) \
    .withColumn("is_bot", is_bot_udf("user_agent"))

def flatten_option_list(options):
    if not options:
        return []
    flattened = []
    for opt in options:
        if not opt:
            continue
        if isinstance(opt, Row):
            opt = opt.asDict()
        if isinstance(opt, dict):
            opt_clean = {k: v for k, v in opt.items() if v not in (None, "", [], {})}
            if opt_clean:
                flattened.append(opt_clean)
    return flattened

def clean_record(record):
    if isinstance(record, Row):
        record = record.asDict()
    elif not isinstance(record, dict):
        return {}
    if "cart_products" in record and record["cart_products"]:
        new_cart_products = []
        for p in record["cart_products"]:
            if isinstance(p, Row):
                p = p.asDict()
            if isinstance(p, dict):
                if "option" in p:
                    p["option"] = flatten_option_list(p.get("option"))
                p = {k: v for k, v in p.items() if v not in (None, "", [], {})}
                if p:
                    new_cart_products.append(p)
        record["cart_products"] = new_cart_products
    if "option" in record and record["option"]:
        record["option"] = flatten_option_list(record["option"])
    record = {k: v for k, v in record.items() if v not in (None, "", [], {})}
    return record

def hash_user_agent(ua: str) -> str:
    return hashlib.md5(ua.encode("utf-8")).hexdigest() if ua else None

def enrich_user_agent(record):
    ua_raw = record.get("user_agent")
    if not ua_raw:
        return None
    return {
        "user_agent_id": hash_user_agent(ua_raw),
        "user_agent_raw": ua_raw,
        "browser_family": record.get("browser_family"),
        "browser_version": record.get("browser_version"),
        "os_family": record.get("os_family"),
        "os_version": record.get("os_version"),
        "device_family": record.get("device_family"),
        "device_brand": record.get("device_brand"),
        "device_model": record.get("device_model"),
        "is_mobile": record.get("is_mobile", False),
        "is_tablet": record.get("is_tablet", False),
        "is_touch_capable": record.get("is_touch_capable", False),
        "is_pc": record.get("is_pc", False),
        "is_bot": record.get("is_bot", False)
    }

def create_temp_table():
    schema_sql = f"""
    CREATE OR REPLACE TABLE `{TEMP_SUMMARY_TABLE}` AS
    SELECT * FROM `{SUMMARY_TABLE}` WHERE 1=0;
    """
    bq_client.query(schema_sql).result()
    log.info(f"Created temporary summary table: {TEMP_SUMMARY_TABLE}")

create_temp_table()


def process_batch(df: DataFrame, batch_id: int):
    rows = [clean_record(r) for r in df.collect()]
    if not rows:
        log.info(f"Batch {batch_id} empty, skip")
        return

    product_map = {}
    for record in rows:
        current_url = record.get("current_url")
        cart_products = record.get("cart_products", [])
        root_pid = record.get("product_id")
        if cart_products:
            for p in cart_products:
                pid = p.get("product_id")
                if pid and current_url:
                    product_map[pid] = current_url
        elif root_pid and current_url:
            product_map[root_pid] = current_url

    product_list = [
        {"product_id": int(pid), "current_url": url, "is_crawl": False}
        for pid, url in product_map.items() if pid and url
    ]
    if product_list:
        errors = bq_client.insert_rows_json(PRODUCTS_TABLE, product_list)
        if errors:
            log.error(f"Batch {batch_id} insert to products errors: {errors}")
        else:
            log.info(f"Batch {batch_id} inserted {len(product_list)} products into {PRODUCTS_TABLE}")

    errors = bq_client.insert_rows_json(TEMP_SUMMARY_TABLE, rows)
    if errors:
        log.error(f"Batch {batch_id} insert to temp summary errors: {errors}")
    else:
        log.info(f"Batch {batch_id} inserted {len(rows)} rows into {TEMP_SUMMARY_TABLE}")

def process_batch_ua(df: DataFrame, batch_id: int):
    rows = [r.asDict() for r in df.collect()]
    ua_list = [enrich_user_agent(r) for r in rows if enrich_user_agent(r)]
    if ua_list:
        errors = bq_client.insert_rows_json(USER_AGENT_TABLE, ua_list)
        if errors:
            log.error(f"Batch {batch_id} insert to user_agent errors: {errors}")
        else:
            log.info(f"Batch {batch_id} inserted {len(ua_list)} user_agents")

query_summary = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/summary_tmp") \
    .start()

query_ua = ua_df.writeStream \
    .foreachBatch(process_batch_ua) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/user_agent_bq") \
    .start()


storage_client = storage.Client.from_service_account_json("/spark/BigDataProject/key.json")

def export_and_delete_temp_table():
    bucket_name = "big_data_project_final"
    prefix = "bronze/summary/output-"

    now_ts = int(time.time() * 1000)
    export_uri = f"gs://{bucket_name}/{prefix}{now_ts}-*.parquet"

    sql = f"""
    EXPORT DATA OPTIONS(
    uri='{export_uri}',
    format='PARQUET',
    overwrite=true
    )
    AS
    SELECT * FROM `{TEMP_SUMMARY_TABLE}`;
    """

    logging.info(f"Exporting BigQuery summary table to GCS with URI: {export_uri}")
    job = bq_client.query(sql)
    job.result()
    logging.info("Export completed successfully to GCS.")

    bq_client.delete_table(TEMP_SUMMARY_TABLE, not_found_ok=True)
    logging.info(f"Deleted temporary table: {TEMP_SUMMARY_TABLE}")
    
EXPORT_FLAG_FILE = "/tmp/export_trigger.txt"

def manual_export_watcher():
    log.info("Manual export watcher started. Create file '/tmp/export_trigger.txt' to trigger export.")
    while True:
        if os.path.exists(EXPORT_FLAG_FILE):
            log.info("Manual export triggered by file flag...")
            try:
                export_and_delete_temp_table()
                log.info("Manual export completed.")
            except Exception as e:
                log.error(f"Export failed: {e}")
            finally:
                os.remove(EXPORT_FLAG_FILE)
        time.sleep(3)

watcher_thread = threading.Thread(target=manual_export_watcher, daemon=True)
watcher_thread.start()

def graceful_shutdown(signum, frame):
    log.info(f"Received termination signal ({signum}), shutting down Spark gracefully...")
    try:
        query_summary.stop()
        query_ua.stop()
        spark.stop()
    finally:
        sys.exit(0)

signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)

query_summary.awaitTermination()
query_ua.awaitTermination()