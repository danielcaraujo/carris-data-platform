from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import types as T


class ConvertArrayColumnsTask:
    """Task class for converting array columns in BigQuery staging tables"""

    def __init__(self, project_id, dataset_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        """Create and configure Spark session"""
        conf = SparkConf()
        conf.set(
            "spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2",
        )

        return (
            SparkSession.builder.appName("ConvertArrayColumnsTask")
            .config(conf=conf)
            .getOrCreate()
        )

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
        df.write.format("bigquery").option(
            "table", f"{self.project_id}.{self.dataset_id}.{destination_table}"
        ).option("writeMethod", "direct").option("allowSchemaUpdates", "true").option(
            "createDisposition", "CREATE_IF_NEEDED"
        ).option(
            "writeDisposition", "WRITE_TRUNCATE"
        ).partitionBy(
            "partition_date"
        ).mode(
            write_mode
        ).save()

    def process_table(self, table_name, columns_to_be_):
        """
        Process a BigQuery table by reading it and converting JSON arrays
        to proper arrays.

        Args:
            spark: SparkSession object
            table_name: Name of the table to read from BigQuery
            columns_to_be_: List of attributes/columns for the table

        Returns:
            Processed DataFrame with converted arrays
        """

        df = (
            self.spark.read.format("bigquery")
            .option(
                "table", f"{self.project_id}.{self.dataset_id}.staging_{table_name}"
            )
            .load()
        )

        for col in columns_to_be_:
            if col in df.columns:
                df = df.withColumn(
                    col, F.from_json(F.col(col), T.ArrayType(T.StringType()))
                )
                print(f"Converted {col} to array column {col + ' array'}")

        self.write_to_bigquery(df, f"{table_name}_converted")

    def run(self, tables):
        """Run the complete task (for all tables)"""
        try:
            for table in tables:
                self.process_table(table["table_name"],
                                   table["columns_to_be_"])
            print("Task completed successfully!")

        except Exception as e:
            print(f"Task failed: {str(e)}")
            raise

        finally:
            self.spark.stop()


tables = [
    {
        "table_name": "stops",
        "columns_to_be_": ["facilities", "line_ids", "pattern_ids", "route_ids"],
    },
    {
        "table_name": "routes",
        "columns_to_be_": [
            "district_ids",
            "facilities",
            "locality_ids",
            "municipality_ids",
            "pattern_ids",
            "region_ids",
            "stop_ids",
        ],
    },
    {
        "table_name": "lines",
        "columns_to_be_": [
            "district_ids",
            "facilities",
            "locality_ids",
            "municipalitiy_ids",
            "pattern_ids",
            "region_ids",
            "route_ids",
            "stop_ids",
        ],
    },
]
task = ConvertArrayColumnsTask(
    project_id="data-eng-dev-437916", dataset_id="applied_project_staging_grupo_1"
)
task.run(tables)
