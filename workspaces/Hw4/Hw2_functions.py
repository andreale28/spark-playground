import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, regexp_extract, to_date, \
    when, sum, lit, input_file_name


def build_spark():
    """
    Builds and configures a Spark session with the specified settings.

    Returns:
        SparkSession: The configured Spark session.
    """
    builder = (
        SparkSession.builder.appName("homework1")
        .config("spark.driver.memory", "16g")
        .config("spark.driver.cores", 3)
        .config("sql.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark


# Just run this function one time
def read_log_content(
        spark: SparkSession,
        data_dir: str,
        log_dir: str,
        parquet_dir: str,
        write_parquet: bool = True,
) -> DataFrame:
    """
    Read log content from a given directory and transform it into a DataFrame.

    Args:
        spark (SparkSession): The SparkSession object.
        data_dir (str): The directory path where the log content is located.
        log_dir (str): The name of the log directory.
        parquet_dir (str): The directory path where the parquet files will be written.
        write_parquet (bool, optional): Whether to write the DataFrame as parquet files or not.
            Defaults to True.

    Returns:
        DataFrame: The transformed log content as a DataFrame.
    """

    df = (
        spark.read.json(f"{data_dir}/{log_dir}")
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
        .filter(col("Contract") != "0")
    )
    if write_parquet:
        df.write.parquet(
            path=f"{data_dir}/{parquet_dir}",
            mode="overwrite",
            partitionBy="Date",
            compression="zstd"
        )

    return df


def summarize_by_manual_pivot(
        df: DataFrame,
        app_names: list[str],
        column_names: list[str]
) -> DataFrame:
    """
    Generate a summarized DataFrame by manually pivoting the provided DataFrame.

    Parameters:
        df (DataFrame): The DataFrame to be summarized.
        app_names (list[str]): A list of application names.
        column_names (list[str]): A list of column names.

    Returns:
        DataFrame: The summarized DataFrame.

    """

    whens = [when(col("AppName") == app_name, col("TotalDuration")).otherwise(lit(0)).alias(f"{column_name}")
             for app_name, column_name in zip(app_names, column_names)
             ]

    exprs = [sum(x).alias(f"{x}") for x in column_names]

    return (
        df
        .select(
            col("Contract"),
            *whens
        )
        .groupby("Contract")
        .agg(*exprs)
        .orderBy("TVDuration", ascending=False)
    )


def summarize_by_supported_pivot(
        df: DataFrame,
        app_names: list[str],
        column_names: list[str]
) -> DataFrame:
    """
    Generate a summarized DataFrame by applying a pivot operation on the given DataFrame.

    Args:
        df (DataFrame): The input DataFrame to be summarized.
        app_names (list[str]): A list of application names to be used for pivoting the DataFrame.
        column_names (list[str]): A list of column names to be used for generating expression aliases.

    Returns:
        DataFrame: The summarized DataFrame after applying the pivot operation.
    """
    exprs = [col(x).alias(f"{y}") for x, y in zip(app_names, column_names)]

    return (
        df
        .groupby("Contract")
        .pivot("AppName", app_names)
        .sum("TotalDuration")
        .select(
            col("Contract"),
            *exprs
        )
        .orderBy("TVDuration", ascending=False)
    )


if __name__ == '__main__':

    log_path = "log_content/*.json"
    data_path = "/data/"
    parquet_path = "parquet_log_content.parquet"

    app_names = [
        "CHANNEL",
        "VOD",
        "KPLUS",
        "CHILD",
        "RELAX",
    ]
    column_names = ["TVDuration", "MovieDuration", "SportDuration",
                    "ChildDuration", "RelaxDuration"]

    spark = build_spark()
    print("==Job started==")

    # check if parquet_path exists or not
    if not os.path.exists(f"{data_path}/{parquet_path}"):
        read_log_content(spark, data_path, log_path, parquet_path)

    df = (
        spark.read.parquet(
            f"{data_path}/{parquet_path}"
        )
        .filter(col("Contract") != "0")
    )

    print("==Data loaded==")

    # result = summarize_by_manual_pivot(spark, df, app_names, column_names)
    result = summarize_by_supported_pivot(df, app_names, column_names)
    print("==Job completed==")
    spark.stop()
