from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import datetime

def create_spark_session(app_name="Daily Ingest"):
    """
    Creates a SparkSession with dynamic partition overwrite enabled.
    """
    spark = SparkSession.builder \
        .master('local') \
        .appName(app_name) \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()
    return spark

def upload_parquet_to_gcs(spark, input_path: str, output_path: str, execution_date=None):
    """
    Reads a Parquet file, adds a partition column with the execution date,
    and writes it to a GCS bucket in partitioned Parquet format.
    
    :param spark: SparkSession object
    :param input_path: Path to the input Parquet file (local or GCS)
    :param output_path: GCS path where the output will be stored
    :param execution_date: Optional string in 'YYYY-MM-DD' format to use as partition
    """
    if execution_date is None:
        execution_date = datetime.date.today().isoformat()

    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        return

    # Add a partition column with the execution date
    df = df.withColumn("partition_date", lit(execution_date))

    # Write the DataFrame to GCS in partitioned Parquet format
    df.write \
        .mode("overwrite") \
        .partitionBy("partition_date") \
        .parquet(output_path)

    print(f"Parquet written to: {output_path}/partition_date={execution_date}/")

if __name__ == "__main__":
    # Replace with the actual input file path (local or GCS)
    input_path = "parquet_local.parquet"
    output_path = "gs://applied-project/grupo-1/datalake"

    spark = create_spark_session()
    upload_parquet_to_gcs(spark, input_path, output_path)