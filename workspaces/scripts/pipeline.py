import json
import logging
import time
from abc import abstractmethod, ABC
from typing import Optional, Dict

import pyspark
from delta import DeltaTable
from pandera.api.pyspark.model import DataFrameModel
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    col,
    to_date,
    regexp_extract,
    input_file_name,
    when,
    sort_array,
    array,
    struct,
    max,
    datediff,
    count_distinct,
    lit,
    ntile,
    min,
    round,
    sum,
)
from pyspark.sql.types import StructType, StructField, StringType, LongType

from workspaces.scripts.table_schema import table_path, validated_table_schema


class StandardETL(ABC):
    def __init__(
        self,
        storage_path: Optional[str] = None,
        json_path: Optional[str] = None,
    ):
        self.STORAGE_PATH = storage_path or "s3a://sparkplayground"
        self.JSON_PATH = json_path or "log_content"

    def get_json_log(self, spark: SparkSession):
        json_storage = f"{self.STORAGE_PATH}/{self.JSON_PATH}"
        schema = StructType(
            [
                StructField("_id", StringType(), True),
                StructField("_index", StringType(), True),
                StructField("_score", LongType(), True),
                StructField(
                    "_source",
                    StructType(
                        [
                            StructField("AppName", StringType(), True),
                            StructField("Contract", StringType(), True),
                            StructField("Mac", StringType(), True),
                            StructField("TotalDuration", LongType(), True),
                        ]
                    ),
                    True,
                ),
                StructField("_type", StringType(), True),
            ]
        )
        return spark.read.json(
            path=json_storage, pathGlobFilter="*.json", schema=schema
        ).select(
            col("_index").alias("Index"),
            col("_type").alias("Type"),
            col("_id").alias("Id"),
            col("_score").alias("Score"),
            col("_source.*"),
            to_date(regexp_extract(input_file_name(), r"\d{8}", 0), "yyyyMMdd").alias(
                "Date"
            ),
        )

    def validate_data(
        self,
        input_data: DataFrame,
        validate_schema: DataFrameModel,
    ) -> None:
        validation_df = validate_schema.validate(check_obj=input_data)
        error_df = validation_df.pandera.errors
        if error_df is not None:
            logging.info("Successfully validate input dataframe without errors")
        else:
            df_out_errors = json.dumps(dict(error_df), indent=4)
            logging.error(
                f"Successfully validate input dataframe with errors {df_out_errors}"
            )

        return None

    def publish_data(
        self,
        spark: SparkSession,
        input_data: DataFrame,
        table_path: str,
        optimize: bool = False,
        **options,
    ) -> None:
        compact = options.get("compact", False)
        zorder = options.get("zorder", False)
        zorder_columns = options.get("zorder_columns", None)
        input_data.write.format("delta").option("compression", "zstd").mode(
            "overwrite"
        ).save(table_path)
        logging.info(f"Saved table into data lake at {table_path}")
        if optimize:
            delta_table = DeltaTable.forPath(spark, table_path)
            if compact:
                compact_result = delta_table.optimize().executeCompaction()
                logging.info("Successfully optimized table by compaction")
                print(
                    json.dumps(
                        json.loads(compact_result.toJSON().collect()[0]), indent=4
                    )
                )
            if zorder:
                zorder_result = delta_table.optimize().executeZOrderBy(zorder_columns)
                logging.info("Successfully optimized table by z-ordering")
                print(
                    json.dumps(
                        json.loads(zorder_result.toJSON().collect()[0]), indent=4
                    )
                )

    @abstractmethod
    def etl_bronze_layer(self, spark: SparkSession, **options) -> DataFrame:
        pass

    @abstractmethod
    def etl_silver_layer(self, spark: SparkSession, **options) -> DataFrame:
        pass

    @abstractmethod
    def etl_gold_layer(self, spark: SparkSession, **options) -> DataFrame:
        pass

    def run(self, spark: SparkSession, **options) -> None:
        partition = options.get("partition")
        app_names = options.get("app_names")
        column_names = options.get("column_names")
        reported_date = options.get("reported_date")
        validated_table_schema: dict = options.get("validated_table_schema")

        self.etl_bronze_layer(
            spark, partition=partition, validated_table_schema=validated_table_schema
        )
        logging.info("Successfully WAP bronze layer")

        self.etl_silver_layer(
            spark,
            app_names=app_names,
            column_names=column_names,
            reported_date=reported_date,
            validated_table_schema=validated_table_schema,
        )
        logging.info("Successfully WAP silver layer")

        self.etl_gold_layer(spark, validated_table_schema=validated_table_schema)
        logging.info("Successfully WAP gold layer")


class LogETL(StandardETL):
    def __init__(self, table_path: Dict[str, str]) -> None:
        self.TABLE_PATH = table_path
        super().__init__()

    def etl_bronze_layer(self, spark: SparkSession, **options):
        # validated_table_schema = options.get("validated_table_schema")
        df = self.get_json_log(spark)
        # self.validate_data(df, validated_table_schema.get("bronze_table"))
        self.publish_data(
            spark,
            df,
            self.TABLE_PATH.get("bronze"),
            optimize=True,
            compact=True,
            zorder=True,
            zorder_columns=["Contract", "Date"],
        )
        return df

    def get_rfm_table(
        self, spark: SparkSession, input_data: DataFrame, **options
    ) -> DataFrame:
        reported_date = options.get("reported_date")
        total_date = input_data.select(col("Date")).distinct().count()

        b = input_data.groupBy("Contract").agg(max(col("Date")).alias("LatestDate"))
        recency_window = Window.partitionBy("Contract").orderBy("Recency")
        frequency_window = Window.partitionBy("Contract").orderBy("Frequency")
        monetary_duration_window = Window.partitionBy("Contract").orderBy(
            "MonetaryDuration"
        )

        rfm = (
            input_data.withColumn(
                "ReportedDate", to_date(lit(reported_date), "yyyyMMdd")
            )
            .join(b, on="Contract")
            .groupBy(col("Contract"))
            .agg(
                min(datediff(col("ReportedDate"), col("LatestDate"))).alias("Recency"),
                round(count_distinct(col("Date")) / lit(total_date) * 100, 2).alias(
                    "Frequency"
                ),
                sum(col("TotalDuration")).alias("MonetaryDuration"),
            )
            .withColumns(
                {
                    "RecencyScore": ntile(4).over(recency_window),
                    "FrequencyScore": ntile(4).over(frequency_window),
                    "MonetaryDurationScore": ntile(4).over(monetary_duration_window),
                }
            )
        )
        return rfm

    def etl_silver_layer(self, spark: SparkSession, **options) -> DataFrame:
        app_names = options.get("app_names")
        column_names = options.get("column_names")
        reported_date = options.get("reported_date")
        # validated_table_schema = options.get("validated_table_schema")

        input_data = spark.read.format("delta").load(path=self.TABLE_PATH.get("bronze"))
        if len(app_names) != len(column_names):
            raise ValueError(
                "The lengths of `app_names` and `column_names` must be the same."
            )

        whens = pyspark.sql.functions
        for app_name, column_name in zip(app_names, column_names):
            whens = whens.when(col("AppName") == app_name, column_name)
        whens = whens.otherwise("Unknown").alias("Type")

        rfm_df = self.get_rfm_table(spark, input_data, reported_date=reported_date)
        self.publish_data(spark, rfm_df, self.TABLE_PATH.get("rfm"))

        pivot_df = (
            input_data.select(col("Contract"), col("TotalDuration"), whens)
            .filter(
                (col("Contract") != "0")
                & (col("Type") != "Unknown")
                & (col("TotalDuration") > 0)
            )
            .groupBy("Contract")
            .pivot("Type")
            .sum("TotalDuration")
        )

        watch_type = ["Child", "Movie", "Relax", "Sport", "TV"]
        # get columns name without the Contract column
        # use struct to get {columns, duration} and use the sort_array function
        # to sort the array in descending order
        columns = pivot_df.columns[1:]
        temp = sort_array(
            array(
                *[
                    struct(col(c).alias("v"), lit(v).alias("k"))
                    for c, v in zip(columns, watch_type)
                ]
            ),
            asc=False,
        )

        silver_df = pivot_df.select(
            col("Contract"),
            col("TVDuration"),
            col("ChildDuration"),
            col("MovieDuration"),
            col("RelaxDuration"),
            col("SportDuration"),
            temp[0]["k"].alias("MostWatch"),
            when(temp[1]["v"].isNotNull(), temp[1]["k"])
            .otherwise(None)
            .alias("SecondMostWatch"),
            when(temp[2]["v"].isNotNull(), temp[2]["k"])
            .otherwise(None)
            .alias("ThirdMostWatch"),
        )
        # self.validate_data(silver_df, validated_table_schema.get("silver_table"))
        self.publish_data(spark, silver_df, self.TABLE_PATH.get("silver"))

        return silver_df

    def etl_gold_layer(self, spark: SparkSession, **options) -> DataFrame:
        validated_table_schema = options.get("validated_table_schema")

        input_data = spark.read.format("delta").load(self.TABLE_PATH.get("silver"))
        rfm_df = spark.read.format("delta").load(self.TABLE_PATH.get("rfm"))

        gold_df = input_data.join(rfm_df, on="Contract").select(
            col("Contract"),
            col("TVDuration"),
            col("ChildDuration"),
            col("MovieDuration"),
            col("RelaxDuration"),
            col("SportDuration"),
            col("MostWatch"),
            col("SecondMostWatch"),
            col("ThirdMostWatch"),
            col("RecencyScore"),
            col("FrequencyScore"),
            col("MonetaryDurationScore"),
        )
        self.validate_data(gold_df, validated_table_schema.get("gold_table"))
        self.publish_data(spark, gold_df, self.TABLE_PATH.get("gold"))

        return gold_df


def main():
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
        "TVDuration",
        "TVDuration",
        "MovieDuration",
        "MovieDuration",
        "MovieDuration",
        "SportDuration",
        "ChildDuration",
        "RelaxDuration",
    ]
    reported_date = "20220501"
    start_time = time.time()
    logging.basicConfig(level=logging.INFO)
    spark = (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("LogETL")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    task = LogETL(table_path=table_path)
    task.run(
        spark,
        partition="Date",
        app_names=app_names,
        column_names=column_names,
        reported_date=reported_date,
        validated_table_schema=validated_table_schema,
    )
    logging.info("Done ETL")
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\nElapsed time: {elapsed_time}")


if __name__ == "__main__":
    main()
