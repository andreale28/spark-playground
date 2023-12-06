import logging
from typing import List

from delta import DeltaTable
from pyspark.sql import SparkSession


class TableManager:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def vacuum_table(self, table_path: str, retention_days: int):
        """
        Vacuums the data in a Delta table after compaction.
        Args:
            table_path (str): The path to the Delta table.
            retention_days (int): The number of days to retain the data.
        Returns:
            None
        Raises:
            Exception: If the vacuum operation fails.
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, table_path)
            logging.info(f"Vacuuming data after compaction at {table_path}")

            return delta_table.vacuum(retention_days)

        except Exception as e:
            logging.error(f"Failed to vacuum {table_path}" f" due to {e}")

    def create_table(
        self,
        table_name: str,
        columns: List[tuple],
        table_path: str,
        is_partition=False,
        **options,
    ) -> DeltaTable:
        """
        Create a Delta table with the given name, columns, and table path.
        Args:
            table_name: The name of the table.
            columns: A list of tuples representing the columns of the table. Each tuple should contain the name and
            data type of column.
            table_path: The path where the table will be created.
        Returns:
            The created DeltaTable object.
        Raises:
            Exception: If the table creation fails.
        """
        try:
            table_builder = (
                DeltaTable.createIfNotExists(self.spark)
                .tableName(table_name)
                .location(table_path)
            )

            for column in columns:
                table_builder = table_builder.addColumn(column[0], column[1])

            if is_partition:
                partition_by = options.get("partition_by")
                table_builder = table_builder.partitionedBy(partition_by)

            table = table_builder.execute()

            logging.info(f"Delta table {table_name} was created at {table_path}")

            return table
        except Exception as e:
            logging.error(f"Failed to create table due to {e}")



