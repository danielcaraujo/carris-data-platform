from pyspark.sql import SparkSession, DataFrame
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import types as T

# 1. Lê os Parquet do GCS
#parquet_path = "gs://applied-project/grupo-1/raw/stops/*/*.parquet"  # ajusta conforme necessário
#spark = SparkSession.builder.getOrCreate()
#df = spark.read.parquet(parquet_path)

class ExplodeArrayColumnsTask:
    """Pipeline class for exploding array columns in BigQuery staging tables"""
    
    def __init__(self, project_id, dataset_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self):
        """Create and configure Spark session"""
        conf = SparkConf()
        conf.set("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2")
        
        return SparkSession.builder \
            .appName("ExplodeArrayColumnsTask") \
            .config(conf=conf) \
            .getOrCreate()

    def write_to_bigquery(self, df, table_name: str, write_mode: str = "overwrite"):
        """ 
        Write DataFrame to BigQuery staging table 
        Args:
            df: DataFrame to write
            table_name: Name of the BigQuery staging table
            write_mode: BigQuery write mode (overwrite, append)
        """  
        destination_table = f"staging_{table_name}"
        
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

    def process_table(self, table_name, columns_to_be_exploded):
        """
        Process a BigQuery table by reading it and converting JSON arrays to proper arrays,
        then exploding them into individual rows.
        
        Args:
            spark: SparkSession object
            table_name: Name of the table to read from BigQuery
            columns_to_be_exploded: List of attributes/columns for the table
        
        Returns:
            Processed DataFrame with exploded arrays
        """
        
        # 1. Read from BigQuery
        df = self.spark.read.format("bigquery").option("table", f"{self.project_id}.{self.dataset_id}.staging_{table_name}").load()
        
        # 2. Convert JSON-string arrays to real arrays
        json_array_cols = columns_to_be_exploded
        for col in json_array_cols:
            if col in df.columns:
                df = df.withColumn(
                    col + "_arr", 
                    F.from_json(F.col(col), T.ArrayType(T.StringType()))
                )
                print(f"Converted {col} to array column {col + '_arr'}")
        
        # 3. Replace empty arrays with ["NA"] to ensure explode doesn't generate null
        array_cols = map(lambda x: x + "_arr", json_array_cols)
        for array_col in array_cols:
            if array_col in df.columns:
                df = df.withColumn(
                    array_col,
                    F.when(
                        (F.col(array_col).isNull()) | (F.size(F.col(array_col)) == 0),
                        F.array(F.lit("NA"))
                    ).otherwise(F.col(array_col))
                )
                print(f"Replaced empty arrays in {array_col} with ['NA']")
        
        # 4. Explode arrays to create individual columns
        explode_map = {
            'line_ids_arr': 'line_id',
            'pattern_ids_arr': 'pattern_id',
            'route_ids_arr': 'route_id',
            'facilities_arr': 'facility_id',
            'district_ids_arr': 'district_id',
            'locality_ids_arr': 'locality_id',
            'municipality_ids_arr': 'municipality_id',
            'region_ids_arr': 'region_id',
            'stop_ids_arr': 'stop_id'
        }

        # 5. Explode each array column into a new column 
        for arr, new_col in explode_map.items():
            if arr in df.columns:
                df = df.withColumn(new_col, F.explode(F.col(arr)))
                print(f"Exploded {arr} into {new_col}")
        
        # 6. Drop original array columns and any JSON array columns
        df = df.drop(*[col for col in df.columns if col.endswith('_arr')], *json_array_cols)
        print(f"Dropped original array columns: {json_array_cols} and exploded array columns")
        
        # 7. Drop original columns
        df = df.drop(*columns_to_be_exploded)
        print(f"Dropped original JSON array columns: {columns_to_be_exploded}")
        
        self.write_to_bigquery(df, f"{table_name}_exploded")

    def run(self, tables):
        """Run the complete task (for all tables)"""
        try:
            for table in tables:
                self.process_table(table["table_name"], table["columns_to_be_exploded"])
            print("Task completed successfully!")
        
        except Exception as e:
            print(f"Task failed: {str(e)}")
            raise
        
        finally:
            self.spark.stop()
            
tables = [
    {
        "table_name": "stops",
        "columns_to_be_exploded": ["facilities", "line_ids", "pattern_ids", "route_ids"]
    },
    {
        "table_name": "routes",
        "columns_to_be_exploded": ["district_ids", "facilities", "locality_ids", "municipality_ids", "pattern_ids", "region_ids", "stop_ids"]
    },
    # {
    #     "table_name": "lines",
    #     "columns_to_be_exploded": ["district_ids", "facilities", "locality_ids", "municipalitiy_ids", "pattern_ids", "region_ids", "route_ids", "stop_ids"]
    # }
]
task = ExplodeArrayColumnsTask(
    project_id="data-eng-dev-437916",
    dataset_id="applied_project_staging_grupo_1"
)
task.run(tables)
