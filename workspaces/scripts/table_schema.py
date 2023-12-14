import pandera.pyspark as pa
from pandera.pyspark import DataFrameModel
from pyspark.sql.types import StringType, LongType, DateType, IntegerType

STORAGE_PATH = "s3a://sparkplayground/"
table_path = {
    "bronze": f"{STORAGE_PATH}/delta/bronze",
    "silver": f"{STORAGE_PATH}/delta/silver",
    "gold": f"{STORAGE_PATH}/delta/gold",
    "rfm": f"{STORAGE_PATH}/delta/silver/rfm",
}


class BronzeSchema(DataFrameModel):
    Index: StringType() = pa.Field(nullable=True)
    Type: StringType() = pa.Field(nullable=True)
    Id: StringType() = pa.Field(nullable=True)
    Score: LongType() = pa.Field(nullable=True)
    AppName: StringType() = pa.Field(nullable=True)
    Contract: StringType() = pa.Field(nullable=True)
    MAC: StringType() = pa.Field(nullable=True)
    TotalDuration: LongType() = pa.Field(nullable=True)
    Date: DateType() = pa.Field(nullable=True)


class SilverSchema(DataFrameModel):
    Contract: StringType() = pa.Field(nullable=True)
    TVDuration: LongType() = pa.Field(nullable=True)
    ChildDuration: LongType() = pa.Field(nullable=True)
    MovieDuration: LongType() = pa.Field(nullable=True)
    RelaxDuration: LongType() = pa.Field(nullable=True)
    SportDuration: LongType() = pa.Field(nullable=True)
    MostWatch: StringType() = pa.Field(nullable=True)
    SecondMostWatch: StringType() = pa.Field(nullable=True)
    ThirdMostWatch: StringType() = pa.Field(nullable=True)


class GoldSchema(DataFrameModel):
    Contract: StringType() = pa.Field(nullable=True)
    TVDuration: LongType() = pa.Field(nullable=True)
    ChildDuration: LongType() = pa.Field(nullable=True)
    MovieDuration: LongType() = pa.Field(nullable=True)
    RelaxDuration: LongType() = pa.Field(nullable=True)
    SportDuration: LongType() = pa.Field(nullable=True)
    MostWatch: StringType() = pa.Field(nullable=True)
    SecondMostWatch: StringType() = pa.Field(nullable=True)
    ThirdMostWatch: StringType() = pa.Field(nullable=True)
    RecencyScore: IntegerType() = pa.Field(
        nullable=True, in_range={"min_value": 1, "max_value": 4}
    )
    FrequencyScore: IntegerType() = pa.Field(
        nullable=True, in_range={"min_value": 1, "max_value": 4}
    )
    MonetaryDurationScore: IntegerType() = pa.Field(
        nullable=True, in_range={"min_value": 1, "max_value": 4}
    )


validated_table_schema = {
    "bronze_table": BronzeSchema,
    "silver_table": SilverSchema,
    "gold_table": GoldSchema,
}
