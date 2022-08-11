from yetl_flow import (
    yetl_flow,
    IDataflow,
    Context,
    Timeslice,
    TimesliceUtcNow,
    OverwriteSave,
    Save,
)
from pyspark.sql.functions import *
from typing import Type


@yetl_flow(log_level="ERROR")
def spark_write(
    context: Context,
    dataflow: IDataflow,
    timeslice: Timeslice = TimesliceUtcNow(),
    save_type: Type[Save] = None,
) -> dict:
    """Load the demo customer data as is into a raw delta hive registered table.

    this is a test pipeline that can be run just to check everything is setup and configured
    correctly.
    """

    # the config for this dataflow has 2 landing sources that are joined
    # and written to delta table
    # delta tables are automatically created and if configured schema exceptions
    # are loaded syphened into a schema exception table
    df_cust = dataflow.source_df("landing.customer")
    df_prefs = dataflow.source_df("landing.customer_preferences")

    context.log.info("Joining customers with customer_preferences")
    df = df_cust.join(df_prefs, "id", "inner")
    df = df_cust

    dataflow.destination_df("raw.customer", df)


def incremental_load():
    results = spark_write(timeslice=Timeslice(2022, 7, 12))


def full_load():
    results = spark_write(timeslice=Timeslice(2022, "*", "*"), save_type=OverwriteSave)


if __name__ == "__main__":
    full_load()
