from pyspark.sql import SparkSession, DataFrame
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType, TimestampType

schemas = {
    "lines": StructType([
        StructField("color", StringType(), True),
        StructField("district_ids", StringType(), True),
        StructField("facilities", StringType(), True),
        StructField("id", StringType(), True),
        StructField("locality_ids", StringType(), True),
        StructField("long_name", StringType(), True),
        StructField("municipality_ids", StringType(), True),
        StructField("pattern_ids", StringType(), True),
        StructField("region_ids", StringType(), True),
        StructField("route_ids", StringType(), True),
        StructField("short_name", StringType(), True),
        StructField("stop_ids", StringType(), True),
        StructField("text_color", StringType(), True),
        StructField("tts_name", StringType(), True),
        StructField("_endpoint", StringType(), True),
        StructField("ingested_at", TimestampType(), False),
        StructField("partition_date", StringType(), False)
    ]),
    
    "endpoint_routes": StructType([
        StructField("color", StringType(), True),
        StructField("district_ids", StringType(), True),
        StructField("facilities", StringType(), True),
        StructField("id", StringType(), True),
        StructField("line_id", StringType(), True),
        StructField("locality_ids", StringType(), True),
        StructField("long_name", StringType(), True),
        StructField("municipality_ids", StringType(), True),
        StructField("pattern_ids", StringType(), True),
        StructField("region_ids", StringType(), True),
        StructField("short_name", StringType(), True),
        StructField("stop_ids", StringType(), True),
        StructField("text_color", StringType(), True),
        StructField("tts_name", StringType(), True),
        StructField("_endpoint", StringType(), True),
        StructField("ingested_at", TimestampType(), False),
        StructField("partition_date", StringType(), False)
    ]),
    
    "gtfs_routes": StructType([
        StructField("agency_id", StringType(), True),
        StructField("circular", LongType(), True),
        StructField("line_id", LongType(), True),
        StructField("line_long_name", StringType(), True),
        StructField("line_short_name", StringType(), True),
        StructField("line_type", DoubleType(), True),
        StructField("path_type", LongType(), True),
        StructField("route_color", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("route_long_name", StringType(), True),
        StructField("route_short_name", StringType(), True),
        StructField("route_text_color", StringType(), True),
        StructField("route_type", LongType(), True),
        StructField("school", DoubleType(), True),
        StructField("_source_file", StringType(), True),
        StructField("ingested_at", TimestampType(), False),
        StructField("partition_date", StringType(), False)
    ]),
    
    "endpoint_stops": StructType([
        StructField("district_id", StringType(), True),
        StructField("facilities", StringType(), True),
        StructField("id", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("line_ids", StringType(), True),
        StructField("locality_id", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("long_name", StringType(), True),
        StructField("municipality_id", StringType(), True),
        StructField("operational_status", StringType(), True),
        StructField("pattern_ids", StringType(), True),
        StructField("region_id", StringType(), True),
        StructField("route_ids", StringType(), True),
        StructField("short_name", StringType(), True),
        StructField("tts_name", StringType(), True),
        StructField("wheelchair_boarding", BooleanType(), True),
        StructField("_endpoint", StringType(), True),
        StructField("ingested_at", TimestampType(), False),
        StructField("partition_date", StringType(), False)
    ]),
    
    "gtfs_stops": StructType([
        StructField("stop_id", LongType(), True),
        StructField("stop_name", StringType(), True),
        StructField("stop_name_new", StringType(), True),
        StructField("stop_short_name", StringType(), True),
        StructField("stop_lat", DoubleType(), True),
        StructField("stop_lon", DoubleType(), True),
        StructField("operational_status", StringType(), True),
        StructField("areas", StringType(), True),
        StructField("region_id", StringType(), True),
        StructField("region_name", StringType(), True),
        StructField("district_id", LongType(), True),
        StructField("district_name", StringType(), True),
        StructField("municipality_id", LongType(), True),
        StructField("municipality_name", StringType(), True),
        StructField("parish_id", DoubleType(), True),
        StructField("parish_name", DoubleType(), True),
        StructField("locality", StringType(), True),
        StructField("jurisdiction", DoubleType(), True),
        StructField("stop_code", LongType(), True),
        StructField("tts_stop_name", StringType(), True),
        StructField("platform_code", DoubleType(), True),
        StructField("parent_station", DoubleType(), True),
        StructField("location_type", LongType(), True),
        StructField("stop_url", StringType(), True),
        StructField("has_pole", StringType(), True),
        StructField("has_cover", StringType(), True),
        StructField("has_shelter", StringType(), True),
        StructField("shelter_code", StringType(), True),
        StructField("shelter_maintainer", StringType(), True),
        StructField("has_mupi", StringType(), True),
        StructField("has_bench", StringType(), True),
        StructField("has_trash_bin", StringType(), True),
        StructField("has_lighting", StringType(), True),
        StructField("has_electricity", StringType(), True),
        StructField("docking_bay_type", StringType(), True),
        StructField("last_infrastructure_maintenance", DoubleType(), True),
        StructField("last_infrastructure_check", DoubleType(), True),
        StructField("has_flag", StringType(), True),
        StructField("flag_maintainer", DoubleType(), True),
        StructField("has_pip_static", StringType(), True),
        StructField("has_pip_audio", StringType(), True),
        StructField("pip_audio_code", DoubleType(), True),
        StructField("has_pip_realtime", StringType(), True),
        StructField("pip_realtime_code", DoubleType(), True),
        StructField("has_h2oa_signage", StringType(), True),
        StructField("has_schedules", StringType(), True),
        StructField("has_tactile_schedules", StringType(), True),
        StructField("has_network_map", StringType(), True),
        StructField("last_schedules_maintenance", DoubleType(), True),
        StructField("last_schedules_check", DoubleType(), True),
        StructField("has_sidewalk", StringType(), True),
        StructField("sidewalk_type", DoubleType(), True),
        StructField("has_crossing", StringType(), True),
        StructField("has_flat_access", StringType(), True),
        StructField("has_wide_access", StringType(), True),
        StructField("has_tactile_access", StringType(), True),
        StructField("has_abusive_parking", StringType(), True),
        StructField("wheelchair_boarding", LongType(), True),
        StructField("last_accessibility_maintenance", DoubleType(), True),
        StructField("last_accessibility_check", DoubleType(), True),
        StructField("near_health_clinic", LongType(), True),
        StructField("near_hospital", LongType(), True),
        StructField("near_university", LongType(), True),
        StructField("near_school", LongType(), True),
        StructField("near_police_station", LongType(), True),
        StructField("near_fire_station", LongType(), True),
        StructField("near_shopping", LongType(), True),
        StructField("near_historic_building", LongType(), True),
        StructField("near_transit_office", LongType(), True),
        StructField("near_beach", LongType(), True),
        StructField("subway", LongType(), True),
        StructField("light_rail", LongType(), True),
        StructField("train", LongType(), True),
        StructField("boat", LongType(), True),
        StructField("airport", LongType(), True),
        StructField("bike_sharing", LongType(), True),
        StructField("bike_parking", LongType(), True),
        StructField("car_parking", LongType(), True),
        StructField("_source_file", StringType(), True),
        StructField("ingested_at", TimestampType(), False),
        StructField("partition_date", StringType(), False)
    ]),
    
    "stop_times": StructType([
        StructField("arrival_time", StringType(), True),
        StructField("departure_time", StringType(), True),
        StructField("drop_off_type", LongType(), True),
        StructField("pickup_type", LongType(), True),
        StructField("shape_dist_traveled", DoubleType(), True),
        StructField("stop_id", LongType(), True),
        StructField("stop_sequence", LongType(), True),
        StructField("timepoint", LongType(), True),
        StructField("trip_id", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("ingested_at", TimestampType(), False),
        StructField("partition_date", StringType(), False)
    ]),
    
    "trips": StructType([
        StructField("calendar_desc", StringType(), True),
        StructField("direction_id", LongType(), True),
        StructField("pattern_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("service_id", StringType(), True),
        StructField("shape_id", StringType(), True),
        StructField("trip_headsign", StringType(), True),
        StructField("trip_id", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("ingested_at", TimestampType(), False),
        StructField("partition_date", StringType(), False)
    ]),
    
    "periods": StructType([
        StructField("period_id", LongType(), True),
        StructField("period_name", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("ingested_at", TimestampType(), False),
        StructField("partition_date", StringType(), False)
    ]),
    
    "dates": StructType([
        StructField("date", LongType(), True),
        StructField("day_type", LongType(), True),
        StructField("holiday", LongType(), True),
        StructField("notes", StringType(), True),
        StructField("period", LongType(), True),
        StructField("_source_file", StringType(), True),
        StructField("ingested_at", TimestampType(), False),
        StructField("partition_date", StringType(), False)
    ]),
    
    "calendar_dates": StructType([
        StructField("date", LongType(), True),
        StructField("day_type", LongType(), True),
        StructField("exception_type", LongType(), True),
        StructField("holiday", LongType(), True),
        StructField("period", LongType(), True),
        StructField("service_id", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("ingested_at", TimestampType(), False),
        StructField("partition_date", StringType(), False)
    ]),
    
    "shapes": StructType([
        StructField("shape_dist_traveled", DoubleType(), True),
        StructField("shape_id", StringType(), True),
        StructField("shape_pt_lat", DoubleType(), True),
        StructField("shape_pt_lon", DoubleType(), True),
        StructField("shape_pt_sequence", LongType(), True),
        StructField("_source_file", StringType(), True),
        StructField("ingested_at", TimestampType(), False),
        StructField("partition_date", StringType(), False)
    ]),
    
    "municipalities": StructType([
        StructField("district_id", LongType(), True),
        StructField("district_name", StringType(), True),
        StructField("municipality_id", LongType(), True),
        StructField("municipality_name", StringType(), True),
        StructField("municipality_prefix", LongType(), True),
        StructField("region_id", StringType(), True),
        StructField("region_name", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("ingested_at", TimestampType(), False),
        StructField("partition_date", StringType(), False)
    ])
}

def clean_dataframe(df):
    """
    Clean the DataFrame by handling nulls and NaNs
    """

    # Convert floats doubles to a default value (null or NaN to 0)
    for name, dtype in df.dtypes:
        if dtype in ['float', 'double']:
            df = df.withColumn(
                name,
                F.when(F.isnan(F.col(name)) | F.col(name).isNull(), F.lit(0)).otherwise(F.col(name))
            )
    # Cleanup strings (null to "NaN")
    for name, dtype in df.dtypes:
        if dtype == 'string':
            df = df.withColumn(
                name,
                F.when(F.col(name).isNull(), F.lit("UNKNOWN")).otherwise(F.col(name))
            )

    return df

class BucketToBigQueryTask:
    """Pipeline class for copying data from GCS bucket to BigQuery staging tables"""
    
    def __init__(self, project_id, dataset_id, bucket_name):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bucket_name = bucket_name
        self.spark = self._create_spark_session()
        self.bigquery_client = bigquery.Client()  
    
    def _create_spark_session(self):
        """Create and configure Spark session"""
        conf = SparkConf()
        conf.set("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2")
        
        return SparkSession.builder \
            .appName("BucketToBigQueryTask") \
            .config(conf=conf) \
            .getOrCreate()

    def drop_table_if_exists(self, project_id, dataset_id, table_name):
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        try:
            self.bigquery_client.delete_table(table_id)
            print(f"Table {table_id} deleted successfully")
        except NotFound:
            print(f"Table {table_id} not found, nothing to delete")
        
    def process_source(self, table_name, table_source, schema) -> DataFrame:
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
        return self.spark.read\
          .schema(schema) \
          .parquet(source_path)
    
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
        
        df = clean_dataframe(df)
        
        df = df.dropDuplicates()  

        destination_table = f"staging_{table_name}"
        
        self.drop_table_if_exists(self.project_id, self.dataset_id, destination_table)

        # Write to BigQuery
        df.write \
            .format("bigquery") \
            .option("table", f"{self.project_id}.{self.dataset_id}.{destination_table}") \
            .option("writeMethod", "direct") \
            .option("allowSchemaUpdates", "true") \
            .option("createDisposition", "CREATE_IF_NEEDED") \
            .option("writeDisposition", "WRITE_TRUNCATE") \
            .partitionBy("partition_date") \
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
        source_dfs = list(map(lambda source: self.process_source(table_name, source, schemas[table_name if len(table_sources) == 1 else f"{source}_{table_name}"]), table_sources))
        
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
