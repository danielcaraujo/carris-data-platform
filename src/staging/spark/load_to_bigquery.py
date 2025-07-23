from pyspark.sql import SparkSession, DataFrame
from pyspark.conf import SparkConf
from pyspark.sql import functions as F


class BucketToBigQueryTask:
    """Pipeline class for copying data from GCS bucket to BigQuery staging tables"""
    
    def __init__(self, project_id, dataset_id, bucket_name):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bucket_name = bucket_name
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self):
        """Create and configure Spark session"""
        conf = SparkConf()
        conf.set("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2")
        
        return SparkSession.builder \
            .appName("BucketToBigQueryTask") \
            .config(conf=conf) \
            .getOrCreate()

    def process_source(self, table_name, table_source) -> DataFrame:
        """
        Process a single source for a table from bucket to BigQuery staging
        
        Args:
            table_name: Name of the table to process
            table_source: Source type (endpoint, gtfs)
        """
        if table_source == "endpoint":
            source_path = f"gs://{self.bucket_name}/{table_name}/*"
        elif table_source == "gtfs":
            source_path = f"gs://{self.bucket_name}/gtfs/{table_name}/*"
        else:
            raise ValueError(f"Unknown source type: {table_source}")
        
        # Read data from GCS bucket
        return self.spark.read.parquet(source_path)
    
    def write_to_bigquery(self, df: DataFrame, table_name: str, write_mode: str = "overwrite"):
        """ 
        Write DataFrame to BigQuery staging table 
        Args:
            df: DataFrame to write
            table_name: Name of the BigQuery staging table
            write_mode: BigQuery write mode (overwrite, append)
        """  
        
        # Add ingestion timestamp          
        df = df.withColumn("ingested_at", F.current_timestamp())
        destination_table = f"staging_{table_name}"
        
        # Write to BigQuery
        df.write \
            .format("bigquery") \
            .option("table", f"{self.project_id}.{self.dataset_id}.{destination_table}") \
            .option("writeMethod", "direct") \
            .mode(write_mode) \
            .save()
        
        print(f"Successfully processed {table_name} -> {destination_table}")
        
    def process_table(self, table_name, table_sources, write_mode="overwrite"):
        """
        Process a single table from bucket to BigQuery staging
        
        Args:
            table_name: Name of the table to process
            table_sorces: An array of sources for the table (endpoint, gtfs)
            write_mode: BigQuery write mode (overwrite, append)
        """
        source_dfs = list(map(lambda source: self.process_source(table_name, source), table_sources))
        
        if len(source_dfs) == 1:
            self.write_to_bigquery(source_dfs[0], table_name, write_mode)
        else:
          # Write each source DataFrame to its own BigQuery table
          for (df, table_source) in zip(source_dfs, table_sources):
                self.write_to_bigquery(df, f"{table_source}_{table_name}", write_mode)
    
    def run(self, tables, write_mode="overwrite"):
        """Run the complete task (for all tables)"""
        try:
            for table in tables:
                self.process_table(table["name"], table["sources"], write_mode)
            print("Pipeline completed successfully!")
        
        except Exception as e:
            print(f"Pipeline failed: {str(e)}")
            raise
        
        finally:
            self.spark.stop()

# Usage example for the class-based approach:
"""
task = BucketToBigQueryTask(
    project_id="your-project-id",
    dataset_id="your-dataset-id", 
    bucket_name="your-bucket-name"
)

tables = [{
  "name":"lines",
  "sources": ["endpoint"],
}, {  
  "name":"stops",
  "sources": ["endpoint", "gtfs"],
}]
task.run(tables, write_mode="overwrite")
"""
task = BucketToBigQueryTask(
    project_id="data-eng-dev-437916",
    dataset_id="applied_project_staging_grupo_1", 
    bucket_name="applied-project/grupo-1/raw"
)

tables = [{
  "name": "lines",
  "sources": ["endpoint"],
}, {  
  "name": "routes",
  "sources": ["endpoint", "gtfs"],
}, {  
  "name": "stops",
  "sources": ["endpoint", "gtfs"],
}, {
  "name": "stop_times",
  "sources": ["gtfs"],
}, {
  "name": "trips",
  "sources": ["gtfs"],
}, {
  "name": "periods",
  "sources": ["gtfs"],
}, {
  "name": "dates",
  "sources": ["gtfs"],
}, {
  "name": "calendar_dates",
  "sources": ["gtfs"],
}, {
  "name": "shapes",
  "sources": ["gtfs"],
}, {
  "name": "municipalities",
  "sources": ["gtfs"],
}]

task.run(tables, write_mode="overwrite")
