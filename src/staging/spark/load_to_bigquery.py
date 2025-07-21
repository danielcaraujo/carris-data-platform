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
        if table_source == 'endpoint':
            source_path = f"gs://{self.bucket_name}/{table_name}/*"
        elif table_source == 'gtfs':
            source_path = f"gs://{self.bucket_name}/gtfs/{table_name}/*"
        else:
            raise ValueError(f"Unknown source type: {table_source}")
        
        # Read data from GCS bucket
        return self.spark.read.parquet(source_path)
    
    def process_table(self, table_name, table_sources, write_mode="overwrite"):
        """
        Process a single table from bucket to BigQuery staging
        
        Args:
            table_name: Name of the table to process
            table_sorces: An array of sources for the table (endpoint, gtfs)
            write_mode: BigQuery write mode (overwrite, append)
        """
        dfs = list(map(lambda source: self.process_source(table_name, source), table_sources))
        
        # Combine all DataFrames into one
        df = dfs[0]
        for additional_df in dfs[1:]:
          additional_df_new_cols = additional_df.select([col for col in additional_df.columns if col not in df.columns])
          df = df.join(additional_df_new_cols, df.id == additional_df_new_cols.stop_id, "inner")


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
    
    def run_pipeline(self, tables, write_mode="overwrite"):
        """Run the complete ETL pipeline for all tables"""
        try:
            for table in tables:
                self.process_table(table['name'], table['sources'], write_mode)
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
  "name":'lines',
  "sources": ['endpoint'],
}, {  
  "name":'stops',
  "sources": ['endpoint', 'gtfs'],
}]
task.run_pipeline(tables, write_mode="overwrite")
"""
task = BucketToBigQueryTask(
    project_id="data-eng-dev-437916",
    dataset_id="applied_project_staging_grupo_1", 
    bucket_name="applied-project/grupo-1/raw"
)

tables = [{
  "name":'lines',
  "sources": ['endpoint'],
}, {  
  "name":'stops',
  "sources": ['endpoint', 'gtfs'],
}, {
  "name":'stop_times',
  "sources": ['gtfs'],
}, {
  "name":'trips',
  "sources": ['gtfs'],
}, {
  "name":'periods',
  "sources": ['gtfs'],
}, {
  "name":'dates',
  "sources": ['gtfs'],
}, {
  "name":'calendar_dates',
  "sources": ['gtfs'],
}, {
  "name":'shapes',
  "sources": ['gtfs'],
}, {
  "name":'municipalities',
  "sources": ['gtfs'],
}]

task.run_pipeline(tables, write_mode="overwrite")
