from contextlib import ContextDecorator
from yetl_flow import (
    yetl_flow,
    IDataflow,
    Context,
    Timeslice,
    TimesliceUtcNow,
    OverwriteSave,
    Save,
)
from yetl_flow.dataset import Destination
from pyspark.sql import functions as fn
from pyspark.sql.window import Window
from typing import Type
from delta.tables import DeltaTable


@yetl_flow(log_level="ERROR")
def type4_cdc_append(
    context: Context,
    dataflow: IDataflow,
    timeslice: Timeslice = TimesliceUtcNow(),
    save_type: Type[Save] = None,
) -> dict:

    # get the source feed.
    df = dataflow.source_df("landing.cdc_customer")
    df = df.alias("src").drop("_corrupt_record")

    # get the current destination
    dst_name = "raw.cdc_append_customer"
    dst: Destination = dataflow.destinations[dst_name]
    dst_table = DeltaTable.forPath(context.spark, dst.path)
    context.log.info(f"Fetched dst delta table {dst_name} from {dst.path}")

    # get the history destination
    dst_history_name = "raw.cdc_append_customer_history"
    dst_history: Destination = dataflow.destinations[dst_history_name]
    dst_table_history = DeltaTable.forPath(context.spark, dst_history.path)
    context.log.info(
        f"Fetched dst delta table {dst_history_name} from {dst_history.path}"
    )

    ########################################################################################
    # Insert existing current records into the history that will be overwritten
    df_history = dst_table_history.toDF()
    df_current = dst_table.toDF()
    df_incoming = df

    keys = ["i.id", "i.extract_date", "i.load_flag"]
    df_incoming_keys = df_incoming.alias("i").select(*keys)
    df_history_keys = (
        df_history.alias("i")
        .join(df_incoming_keys.alias("o"), "id", "inner")
        .select(*keys)
    )
    df_current_keys = (
        df_current.alias("i")
        .join(df_incoming_keys.alias("o"), "id", "inner")
        .select(*keys)
    )

    df_change_set = df_incoming_keys.union(
        df_history_keys.union(df_current_keys)
    ).distinct()

    window_version = Window.partitionBy("id").orderBy(fn.col("extract_date").desc())
    df_change_set = df_change_set.withColumn("version", fn.rank().over(window_version))


    ########################################################################################
    # Insert existing current records into the history that will be overwritten

    df_current_to_hist = (
        df_current.alias("current")
        .join(
            df_change_set.alias("keys"),
            (df_change_set["id"] == df_current["id"])
            & (df_change_set["extract_date"] == df_current["extract_date"]),
            "inner",
        )
        .where("keys.version != 1 or keys.load_flag = 'D'")
        .select("current.*")
    )
    df_current_to_hist = (
        df_current_to_hist.alias("current")
        .join(
            df_history.alias("hist"),
            (df_history["id"] == df_current_to_hist["id"])
            & (df_history["extract_date"] == df_current_to_hist["extract_date"]),
            "left",
        )
        .where("hist.id is null")
        .select("current.*")
    )

    df_incoming_to_hist = (
        df_incoming.alias("incoming")
        .join(
            df_change_set.alias("keys"),
            (df_change_set["id"] == df_incoming["id"])
            & (df_change_set["extract_date"] == df_incoming["extract_date"]),
            "inner",
        )
        .where("keys.version != 1 or keys.load_flag = 'D'")
        .select("incoming.*")
    )
    df_incoming_to_hist = (
        df_incoming_to_hist.alias("incoming")
        .join(
            df_history.alias("hist"),
            (df_history["id"] == df_incoming_to_hist["id"])
            & (df_history["extract_date"] == df_incoming_to_hist["extract_date"]),
            "left",
        )
        .where("hist.id is null")
        .select("incoming.*")
    )


    (
        df_current_to_hist.write.format(dst_history.format)
        .mode("append")
        .saveAsTable(f"{dst_history.database}.{dst_history.table}")
    )

    (
        df_incoming_to_hist.write.format(dst_history.format)
        .mode("append")
        .saveAsTable(f"{dst_history.database}.{dst_history.table}")
    )

    ########################################################################################
    # Merge into current

    df_incoming_to_current = (
        df_incoming.alias("incoming")
        .join(
            df_change_set.alias("keys"),
            (df_incoming["id"] == df_change_set["id"])
            & (df_incoming["extract_date"] == df_change_set["extract_date"]),
            "inner",
        )
        .where("keys.version = 1")
        .select("incoming.*")
    )

    (
        dst_table.alias("dst")
        .merge(df_incoming_to_current.alias("src"), "src.id = dst.id")
        .whenNotMatchedInsertAll("src.load_flag in ('I','U')")
        .whenMatchedUpdateAll(
            "src.load_flag in ('I','U') AND src.extract_date > dst.extract_date"
        )
        .whenMatchedDelete("src.load_flag in ('D')")
        .execute()
    )



def incremental_load():
    days = [1, 1, 2, 2, 3, 4]
    for d in days:
        results = type4_cdc_append(timeslice=Timeslice(2022, 8, d))


def incremental_backwards_load():

    days = [4, 3, 2, 1]
    for d in days:
        results = type4_cdc_append(timeslice=Timeslice(2022, 8, d))


def incremental_out_of_order_load():

    days = [1, 4, 2, 3]
    for d in days:
        results = type4_cdc_append(timeslice=Timeslice(2022, 8, d))


def full_load():
    results = type4_cdc_append(timeslice=Timeslice(2022, 8, "*"))


if __name__ == "__main__":
    full_load()
