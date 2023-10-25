import time
from datetime import datetime
import logging
import os
from abc import ABC, abstractmethod
from typing import Optional, Any

import pyspark.sql.functions as F
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, regexp_extract, to_date, \
    input_file_name


def measure_execution_time(func):
    """Measures the execution time of a function.

    Args:
      func: The function to measure the execution time of.

    Returns:
      A wrapper function that measures the execution time of the given function and prints the results to the console.
    """
    def wrapper(*args,
                **kwargs
                ):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time for {func.__name__}: {execution_time:.2f} seconds")
        return result

    return wrapper

def build_spark() -> SparkSession:
    """
    Builds and configures a Spark session with the specified settings.

    Returns:
        SparkSession: The configured Spark session.
    """
    builder = (
        SparkSession.builder.appName("homework2")
        .config("spark.driver.memory", "16g")
        .config("spark.driver.cores", 4)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )
    section = configure_spark_with_delta_pip(builder).getOrCreate()

    return section


class StandardETL(ABC):
    def __init__(
            self,
            storage_path: Optional[str] = None,
            parquet_path: Optional[str] = None,
            json_path: Optional[str] = None,
            save_path: Optional[str] = None
    ):
        self.STORAGE_PATH = storage_path or "/Users/sonle/Documents/GitHub/spark-playground/data/"
        self.PARQUET_PATH = parquet_path or "parquet_log_content.parquet"
        self.JSON_PATH = json_path or "log_content/"
        self.SAVE_PATH = save_path or "aggregate_result.parquet"

        if not os.path.exists(self.STORAGE_PATH):
            raise FileNotFoundError(f"{self.STORAGE_PATH} not exists, please check it again")

        if not os.path.exists(os.path.join(self.STORAGE_PATH, self.JSON_PATH)):
            raise FileNotFoundError(f"{self.JSON_PATH} not exists, please check it again")

    def to_parquet(self,
                   spark: SparkSession
                   ):
        """
            Convert a JSON file to Parquet format and save it to a specified location.

            Args:
                spark (SparkSession): The SparkSession object.

            Raises:
                FileNotFoundError: If the storage_path, json_path, or parquet_path does not exist.

            Returns:
                None: This function does not return any value.
        """

        if not os.path.exists(os.path.join(self.STORAGE_PATH, self.PARQUET_PATH)):
            df = (
                spark.read.json(f"{self.STORAGE_PATH}/{self.JSON_PATH}", pathGlobFilter="*.json")
                .select(
                    col("_index").alias("Index"),
                    col("_type").alias("Type"),
                    col("_id").alias("Id"),
                    col("_score").alias("Score"),
                    col("_source.*"),
                    to_date(
                        regexp_extract(input_file_name(), r"\d{8}", 0),
                        "yyyyMMdd"
                    ).alias("Date")
                )
            )
            df.write.parquet(
                path=f"{self.STORAGE_PATH}/{self.PARQUET_PATH}",
                mode="overwrite",
                partitionBy="Date",
                compression="zstd"
            )
            logging.info("Convert log content data from json to parquet format successfully")
        else:
            logging.info(f"{self.PARQUET_PATH} already exists, skip this operation")

        return None

    @abstractmethod
    def extract(self,
                spark: SparkSession,
                **options
                ) -> DataFrame:
        pass

    @abstractmethod
    def transform(self,
                  spark: SparkSession,
                  input_data: DataFrame,
                  **options: Any
                  ) -> DataFrame:
        pass

    @abstractmethod
    def load(self,
             spark: SparkSession,
             input_data: DataFrame,
             **options: Any
             ) -> DataFrame:
        pass

    @measure_execution_time
    def run(self,
            spark: SparkSession,
            **options: Any
            ):

        app_names = options.get("app_names")
        column_names = options.get("column_names")

        self.to_parquet(spark)

        raw_data = self.extract(spark, **options)
        logging.info(
            f"Successfully extracted data from {self.STORAGE_PATH}/{self.JSON_PATH}"
        )

        transformed_data = self.transform(spark, raw_data, app_names=app_names, column_names=column_names)
        logging.info(
            "Successfully transformed data from raw data"
        )
        transformed_data.show(truncate=False)

        self.load(spark, transformed_data)
        logging.info(
            f"Successfully save data at {self.STORAGE_PATH}/{self.SAVE_PATH}"
        )
        return None


class LogETLMethod1(StandardETL):
    def __init__(self,
                 start_date: str | datetime,
                 end_date: str | datetime
                 ):
        self.start_date = start_date
        self.end_date = end_date
        super().__init__()

    def extract(self,
                spark: SparkSession,
                **options
                ) -> DataFrame:
        """
        Extract log data from given directories in a period of time and load it to Spark DataFrame
        Filter out the rows with empty "Contract" column and the row with "UNKNOWN" contract name
        """

        df = (
            spark.read.parquet(f"{self.STORAGE_PATH}/{self.PARQUET_PATH}")
            .filter(
                col("Date").between(self.start_date, self.end_date)
            )
            .filter(
                (col("Contract") != "") & (col("Contract") != "0")
            )
        )

        return df

    def transform(self,
                  spark: SparkSession,
                  input_data: DataFrame,
                  **options: Any
                  ) -> DataFrame:
        """
        Transform the input DataFrame by grouping it by "Contract" column,
        pivoting it by "AppName" using the provided `app_names` list,
        summing the "TotalDuration" column, and selecting a subset of columns.
        The resulting DataFrame is ordered by the "TVDuration" column in descending order.
        Args:
            spark (SparkSession): The SparkSession object.
            input_data (DataFrame): The input DataFrame to be transformed.
            **options (Any): Additional options.
        Returns:
            DataFrame: The transformed DataFrame.
        """

        app_names = options.get("app_names")
        column_names = options.get("column_names")

        if len(app_names) != len(column_names):
            raise ValueError("The lengths of `app_names` and `column_names` must be the same.")

        whens = F
        for app_name, column_name in zip(app_names, column_names):
            whens = whens.when(col("AppName") == app_name, column_name)
        whens = whens.otherwise("Unknown").alias("Type")

        df = (
            input_data
            .select(
                col("Contract"),
                col("TotalDuration"),
                whens
            )
            .filter(
                (col("Contract") != "0") & (col("Type") != "Unknown") & (col("Type") != "Error")
            )
            .groupBy("Contract")
            .pivot("Type")
            .sum("TotalDuration")
        )

        return df

    def load(self,
             spark: SparkSession,
             input_data: DataFrame,
             **options: Any
             ) -> None:
        """
        Load the transformed DataFrame to the specified path
        Args:
            spark (SparkSession: a Spark session
            input_data: trans DataFrame
            save_path: saved result directory
            **options: additional options to pass to the function

        Returns None

        """

        try:
            return (
                input_data
                .write
                .parquet(
                    path=f"{self.STORAGE_PATH}/{self.start_date}_{self.end_date}_{self.SAVE_PATH}",
                    compression="zstd",
                    mode="overwrite"
                )
            )
        except (FileNotFoundError, PermissionError) as e:
            logging.error(f"Error occurred while writing data: {e}")


class LogETLMethod2(StandardETL):
    def __init__(self):
        super().__init__()

    def extract(self,
                spark: SparkSession,
                **options
                ) -> DataFrame:
        """
        Extract log data from given directories and load it to Spark DataFrame
        Filter out the rows with empty "Contract" column and the row with "UNKNOWN" contract name
        Args:
            spark (SparkSession): a SparkSession
            **options:  additional options to pass in

        Returns (DataFrame): a raw DataFrame

        """
        df = (
            spark.read.parquet(f"{self.STORAGE_PATH}/{self.PARQUET_PATH}")
            .filter(
                (col("Contract") != "") & (col("Contract") != "0")
            )
        )

        return df

    def transform(self,
                  spark: SparkSession,
                  input_data: DataFrame,
                  **options: Any
                  ) -> DataFrame:
        """
        Transform the input DataFrame by grouping it by "Contract" column,
        pivoting it by "AppName" using the provided `app_names` list,
        summing the "TotalDuration" column, and selecting a subset of columns.
        The resulting DataFrame is ordered by the "TVDuration" column in descending order.
        Args:
            spark (SparkSession): The SparkSession object.
            input_data (DataFrame): The input DataFrame to be transformed.
            **options (Any): Additional options.
        Returns:
            DataFrame: The transformed DataFrame.
        """

        app_names = options.get("app_names")
        column_names = options.get("column_names")

        if len(app_names) != len(column_names):
            raise ValueError("The lengths of `app_names` and `column_names` must be the same.")

        whens = F
        for app_name, column_name in zip(app_names, column_names):
            whens = whens.when(col("AppName") == app_name, column_name)
        whens = whens.otherwise("Unknown").alias("Type")

        df = (
            input_data
            .select(
                col("Contract"),
                col("TotalDuration"),
                whens
            )
            .filter(
                (col("Contract") != "0") & (col("Type") != "Unknown") & (col("Type") != "Error")
            )
            .groupBy("Contract")
            .pivot("Type")
            .sum("TotalDuration")
        )

        return df

    def load(self,
             spark: SparkSession,
             input_data: DataFrame,
             **options: Any
             ) -> None:
        """
        Load the transformed DataFrame to the specified path
        Args:
            spark (SparkSession: a Spark session
            input_data: trans DataFrame
            save_path: saved result directory
            **options: additional options to pass to the function

        Returns None

        """

        try:
            return (
                input_data
                .write
                .parquet(
                    path=f"{self.STORAGE_PATH}/{self.SAVE_PATH}",
                    compression="zstd",
                    mode="overwrite"
                )
            )
        except (FileNotFoundError, PermissionError) as e:
            logging.error(f"Error occurred while writing data: {e}")


if __name__ == '__main__':
    app_names = [
        "CHANNEL",
        "KPLUS",
        "VOD",
        "FIMS",
        "BHD",
        "SPORT",
        "CHILD",
        "RELAX",
    ]

    column_names = [
        "TV",
        "TV",
        "Movie",
        "Movie",
        "Movie",
        "Sport",
        "Child",
        "Relax",
    ]
    start_date = '2022-04-01'
    end_date = '2022-04-03'

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    logging.info(
        "Successfully built the Spark Session"
    )

    tasks1 = LogETLMethod1(start_date=start_date, end_date=end_date)
    tasks1.run(spark, app_names=app_names, column_names=column_names)

    task2 = LogETLMethod2()
    task2.run(spark, app_names=app_names, column_names=column_names)

    spark.stop()
