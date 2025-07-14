from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import datetime

def create_spark_session(app_name="Daily Ingest"):
    """
    Creates a SparkSession with dynamic overwrite enabled.
    """
    spark = SparkSession.builder \
        .master('local') \
        .appName(app_name) \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()
    return spark

def json_to_partitioned_parquet(spark, json_data, output_path: str, execution_date=None):
    """
    Converts a list of dictionaries (JSON) into a DataFrame,
    adds a partition column with the date, and saves it as Parquet
    """
    if execution_date is None:
        execution_date = datetime.date.today().isoformat()

    if not json_data:
        print("Invalid json data received")
        return

    # Se for dict, coloca numa lista
    if isinstance(json_data, dict):
        json_data = [json_data]

    df = spark.createDataFrame(json_data)
    df = df.withColumn("partition_date", lit(execution_date))

    df.write \
        .mode("overwrite") \
        .partitionBy("partition_date") \
        .parquet(output_path)

    print(f"Parquet gravado em: {output_path}/partition_date={execution_date}/")


if __name__ == "__main__":
    json_data = [
        {"id": 1, "value": 10.0},
        {"id": 2, "value": 15.5},
        {"id": 3, "value": 7.0}
    ]

    spark_session = create_spark_session()

    json_to_partitioned_parquet(spark_session, json_data, 'gs://applied-project/grupo-1/datalake')


"""
Command to setup this:

gcloud dataproc batches submit pyspark gs://applied-project/grupo-1/scripts/extract_to_raw.py --batch pysparkgcstest --deps-bucket=applied-p
roject --region=europe-west1


"""