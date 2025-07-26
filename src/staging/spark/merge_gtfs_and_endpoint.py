from pyspark.sql import SparkSession, DataFrame
from pyspark.conf import SparkConf
from pyspark.sql import functions as F


class MergeGTFSAndEndpointTablesTask:
    """Pipeline class for copying data from GCS bucket to BigQuery staging tables"""
    
    def __init__(self, project_id, dataset_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self):
        """Create and configure Spark session"""
        conf = SparkConf()
        conf.set("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2")
        
        return SparkSession.builder \
            .appName("MergeGTFSAndEndpointTablesTask") \
            .config(conf=conf) \
            .getOrCreate()
    
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
            .option("allowSchemaUpdates", "true") \
            .option("createDisposition", "CREATE_IF_NEEDED") \
            .option("writeDisposition", "WRITE_TRUNCATE") \
            .mode(write_mode) \
            .save()
        
        print(f"Successfully merged {table_name} -> {destination_table}")
        
    def process_table(self, table_name, columns_to_exclude, columns_to_rename, write_mode="overwrite"):
        gtfs_df = self.spark.read.format("bigquery") \
            .option("table", f"{self.project_id}.{self.dataset_id}.staging_gtfs_{table_name}") \
            .load()
        endpoint_df = self.spark.read.format("bigquery") \
            .option("table", f"{self.project_id}.{self.dataset_id}.staging_endpoint_{table_name}") \
            .load()
        
        gtfs_df_new_cols = gtfs_df.select([col for col in gtfs_df.columns if col not in endpoint_df.columns])
        merged_df = endpoint_df.join(gtfs_df_new_cols, endpoint_df.id == gtfs_df_new_cols[f"{table_name[:-1]}_id"], "inner")

        # Exclude specified columns if provided
        if columns_to_exclude:
            merged_df = merged_df.drop(*columns_to_exclude)

        # Rename columns if specified
        if columns_to_rename:
            for rename in columns_to_rename:
                merged_df = merged_df.withColumnRenamed(rename["old_name"], rename["new_name"])

        self.write_to_bigquery(merged_df, table_name, write_mode)
    
    def run(self, tables, write_mode="overwrite"):
        """Run the complete task (for all tables)"""
        try:
            for table in tables:
                self.process_table(table["name"], table["columns_to_exclude"], table["columns_to_rename"], write_mode)
            print("Pipeline completed successfully!")                        
        
        except Exception as e:
            print(f"Pipeline failed: {str(e)}")
            raise
        
        finally:
            self.spark.stop()

# Usage example for the class-based approach:
"""
task = MergeGTFSAndEndpointTablesTask(
    project_id="your-project-id",
    dataset_id="your-dataset-id", 
)

tables = [{
  "name": "routes",
  "columns_to_exclude": []
}, {
  "name": "stops",
  "columns_to_exclude": []
}]
task.run(tables, write_mode="overwrite")
"""
task = MergeGTFSAndEndpointTablesTask(
    project_id="data-eng-dev-437916",
    dataset_id="applied_project_staging_grupo_1", 
)

tables = [{
  "name": "routes",
  "columns_to_exclude": ["color", "text_color", "id", "short_name", "long_name"],
  "columns_to_rename": []
}, {
  "name": "stops",
  "columns_to_exclude": ["id", "lat", "lon", "short_name", "long_name", "tts_name", "stop_name_new"],
  "columns_to_rename": [{
    "old_name": "stop_lat",
    "new_name": "stop_latitude"
  }, {
    "old_name": "stop_lon",
    "new_name": "stop_longitude"
  }]
}]  

task.run(tables, write_mode="overwrite")
