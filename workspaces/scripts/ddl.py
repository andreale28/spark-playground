import logging
from typing import List, Tuple

from delta import DeltaTable
from pyspark.sql import SparkSession


def create_tables(
        spark: SparkSession,
        table_name: str,
        columns: List[Tuple[str, str]],
        partition: bool = False,
        storage_path: str = "s3a://sparkplayground/",
        database: str = "logcontent",
        **options,
):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    spark.sql(f"DROP TABLE IF EXISTS {database}.{table_name}")

    table_builder = (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"{database}.{table_name}")
        .location(f"{storage_path}/delta/{table_name}")
    )
    for column in columns:
        table_builder = table_builder.addColumn(column[0], column[1])

    if partition:
        partition_by = options.get("partition_by")
        table_builder = table_builder.partitionedBy(partition_by)

    table = table_builder.execute()

    logging.info(
        f"Delta table {table_name} created at {storage_path}/delta/{table_name}"
    )


def main():
    BRONZE_COLUMNS = [
        ("Index", "STRING"),
        ("Type", "STRING"),
        ("Id", "STRING"),
        ("Score", "LONG"),
        ("AppName", "STRING"),
        ("Contract", "STRING"),
        ("MAC", "STRING"),
        ("TotalDuration", "LONG"),
        ("Date", "DATE"),
    ]

    SILVER_COLUMNS = [
        ("Contract", "STRING"),
        ("TVDuration", "LONG"),
        ("ChildDuration", "LONG"),
        ("MovieDuration", "LONG"),
        ("RelaxDuration", "LONG"),
        ("SportDuration", "LONG"),
        ("MostWatch", "STRING"),
        ("SecondMostWatch", "STRING"),
        ("ThirdMostWatch", "STRING"),
    ]

    GOLD_COLUMNS = [
        ("Contract", "STRING"),
        ("TVDuration", "LONG"),
        ("ChildDuration", "LONG"),
        ("MovieDuration", "LONG"),
        ("RelaxDuration", "LONG"),
        ("SportDuration", "LONG"),
        ("MostWatch", "STRING"),
        ("SecondMostWatch", "STRING"),
        ("ThirdMostWatch", "STRING"),
        ("RecencyScore", "INT"),
        ("FrequencyScore", "INT"),
        ("MonetaryDurationScore", "INT"),
    ]
    logging.basicConfig(level=logging.INFO)

    spark = (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("Log Content DDL")
        .enableHiveSupport()
        .getOrCreate()
    )
    # bronze table
    create_tables(spark, "bronze", BRONZE_COLUMNS, partition=True, partition_by="Date")
    # silver table
    create_tables(spark, "silver", SILVER_COLUMNS)
    # gold table
    create_tables(spark, "gold", GOLD_COLUMNS)
    logging.info("Successfully created tables")


if __name__ == "__main__":
    main()

# spark-submit --driver-memory 4g --executor-memory 6g --executor-cores 2 --properties-file container/spark/conf/spark-defaults.cn_conf workspaces/scripts/ddl.py