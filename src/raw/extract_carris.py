import requests
import pandas as pd
from datetime import datetime
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import tempfile
import zipfile
import argparse

# Configuração
GCS_BASE = "gs://applied-project/grupo-1/raw"
API_BASE = "https://api.carrismetropolitana.pt/v2"
PARTITION_VALUE = datetime.now().strftime("%Y%m%d")

def get_api(endpoint, is_zip=False, max_retries=3):
    url = f"{API_BASE}{endpoint}"
    headers = {"User-Agent": "Carris-Spark-Ingest/1.0"}
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            if is_zip:
                return r.content
            else:
                return r.json()
        except Exception as e:
            print(f"Request failed for {endpoint} (attempt {attempt + 1} of {max_retries}): {e}")
            if attempt == max_retries - 1:
                print(f"Failed to fetch {endpoint} after {max_retries} attempts. Skipping.")
                return None

def pandas_to_spark(spark, df_pd):
    return spark.createDataFrame(df_pd)

def extract_endpoint_to_spark(spark, endpoint, gcs_dir):
    print(f"Extracting {endpoint}...")
    data = get_api(endpoint)
    if data is None:
        print(f"Skipping {endpoint} due to repeated failures.")
        return
    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict) and 'data' in data:
        df = pd.DataFrame(data['data'])
    else:
        df = pd.json_normalize(data)
    df['_endpoint'] = endpoint
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)
    spark_df = pandas_to_spark(spark, df)
    spark_df = spark_df.withColumn("partition_date", lit(PARTITION_VALUE))
    entity_name = endpoint.replace("/", "_").strip("_")
    output_path = f"{gcs_dir}/{entity_name}"
    spark_df.write.mode("overwrite").partitionBy("partition_date").parquet(output_path)
    print(f"Written {entity_name} to {output_path}")

def extract_gtfs_to_spark(spark, gcs_dir):
    print("Extracting GTFS zip...")
    zip_bytes = get_api("/gtfs", is_zip=True)
    if zip_bytes is None:
        print("Skipping GTFS extraction due to repeated failures.")
        return
    with tempfile.NamedTemporaryFile(suffix='.zip') as tmp:
        tmp.write(zip_bytes)
        tmp.flush()
        with zipfile.ZipFile(tmp.name, 'r') as zip_file:
            for filename in zip_file.namelist():
                if filename.endswith('.txt'):
                    table_name = filename.replace('.txt', '')
                    with zip_file.open(filename) as f:
                        df = pd.read_csv(f, low_memory=False)
                        df['_source_file'] = filename
                        spark_df = pandas_to_spark(spark, df)
                        spark_df = spark_df.withColumn("partition_date", lit(PARTITION_VALUE))
                        output_path = f"{gcs_dir}/gtfs/{table_name}"
                        spark_df.write.mode("overwrite").partitionBy("partition_date").parquet(output_path)
                        print(f"Written GTFS {table_name} to {output_path}")

def discover_facility_endpoints():
    data = get_api("/facilities")
    if data is None:
        print("Could not retrieve /facilities endpoints, skipping sub-endpoints.")
        return []
    facility_types = data.get("available_facilities", [])
    return [f"/facilities/{ft}" for ft in facility_types]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Carris Spark GCS Loader")
    parser.add_argument("--endpoint", type=str, default=None,
                        help="Single API endpoint to extract (e.g. /stops, gtfs, /facilities/schools). If omitted, extracts all endpoints and GTFS.")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("CarrisSparkDirectGCS").getOrCreate()

    if args.endpoint:
        ep = args.endpoint.strip().lower()
        if ep == "gtfs" or ep == "/gtfs":
            extract_gtfs_to_spark(spark, GCS_BASE)
        else:
            extract_endpoint_to_spark(spark, args.endpoint, GCS_BASE)
    else:
        # Batch mode (tudo)
        endpoints = [
            "/municipalities", "/stops", "/lines",
            "/routes", "/facilities"
        ]
        endpoints += discover_facility_endpoints()
        for endpoint in endpoints:
            extract_endpoint_to_spark(spark, endpoint, GCS_BASE)
        extract_gtfs_to_spark(spark, GCS_BASE)
        print("Ingestão Carris completa para GCS.")

    spark.stop()