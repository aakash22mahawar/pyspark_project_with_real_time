from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from delta.tables import DeltaTable


class Transformations:

    def __init__(self, spark):
        self.spark = spark

    def dedup(self, df: DataFrame, dedup_cols: List[str], cdc: str) -> DataFrame:
        """
        Deduplicate records based on business keys and CDC column.
        """

        # Create composite key using dedup columns
        df = df.withColumn(
            "dedupKey",
            concat(*[col(c) for c in dedup_cols])
        )

        # Apply row_number window function to identify latest records
        df = df.withColumn(
            "dedupCounts",
            row_number().over(
                Window.partitionBy("dedupKey").orderBy(col(cdc).desc())
            )
        )

        # Keep only latest record
        df = df.filter(col("dedupCounts") == 1)

        # Drop helper columns
        df = df.drop("dedupKey", "dedupCounts")

        return df

    def process_timestamp(self, df: DataFrame) -> DataFrame:
        """
        Process timestamp column to extract date and time components.
        """

        df = df.withColumn(
            "process_timestamp",
            date_format(current_timestamp(), "dd-MM-yyyy hh:mm:ss a")
        )

        return df
    

    def sales_prod_join(self, df_sales: DataFrame, df_prod: DataFrame) -> DataFrame:
        """
        Join sales with products.
        """

        df_prod = df_prod.filter(col("_corrupt_record").isNull())

        df_prod = df_prod.select(
            col("product_id"),
            col("category"),
            col("unit_price")
        )

        df_join = df_sales.join(
            df_prod,
            on="product_id",
            how="inner"
        )

        # Drop bad rows
        df_join = df_join.filter(
            (col("unit_price") > 0) &
            (to_date(col("date"), "yyyy-MM-dd").isNotNull())
        )


        return df_join

    def upsert(
        self,
        df: DataFrame,
        table_name: str,
        key_cols: List[str],
        cdc: str
    ) -> str:
        """
        Upsert records into Delta table.
        """

        merge_condition = " AND ".join(
            [f"src.{col_name} = tgt.{col_name}" for col_name in key_cols]
        )

        dlt_obj = DeltaTable.forName(
            self.spark,
            f"pyspark_dbt.silver.{table_name}"
        )

        (
            dlt_obj.alias("tgt")
            .merge(
                df.alias("src"),
                merge_condition
            )
            .whenMatchedUpdateAll(
                condition=f"src.{cdc} >= tgt.{cdc}"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        return "Upsert records into Delta table"