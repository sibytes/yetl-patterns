<img src="https://img.shields.io/badge/Python-v3.8-blue">

# YETL (you know, for Spark!) Framework - Patterns & Examples

Lots of examples and patterns for loading data through spark pipelines using YETL. 

Still under development!

`pip install yetl-framework`

Website & Docs: [Yet (another Apache Spark) ETL Framework](https://www.yetl.io/)


## Demo Landing To Raw:

Run [main.py](./main.py). If you're using vscode and the dependencies are setup you should just be able to hit F5 since [.vscode](./.vscode) is included in this repo.
When the execution completes, run `pyspark` in the project root and should should be able to query the result `raw.customer` table that was loaded.  

Check the table properties using `DESCRIBE` and observe partitions and other properties that have been applied automatically by framework.

To clean up the load after a run in order to start fresh run:
```
sh cleanup.sh
```

The demo will also Zorder as per the configuration [demo_landing_to_raw](./config/pipeline/local/demo_landing_to_raw.yaml). Note the framework has a number of features not covered in the demo right now.

Source landing stage data is included in the repo at the following respective location. Delta lake is also configured to write to this location. The delta lake dir will appear after you run the demo. It is not included in the source code and `sh cleanup.sh` will remove it to clean down the delta lake db:
```
./data/delta_lake
./data/landing
```

Configuration for the pipeline for local and databricks is as follows along with the spark and SQL schema's used by the framework.

local & dbx pipeline config:
```
./config/pipeline/local
./config/pipeline/local
```

schema repo:
```
delta lake table -> ./config/schema/deltalake/raw/customer.sql
spark schema - > ./config/schema/spark/landing/
```

### Define a dataflow

```python

from yetl_flow import (
    yetl_flow,
    IDataflow,
    Context,
    Timeslice,
    TimesliceUtcNow,
    OverwriteSave,
    OverwriteSchemaSave,
    Save,
)
from pyspark.sql.functions import *
from typing import Type


@yetl_flow(log_level="ERROR")
def demo_landing_to_raw(
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
    df = df.withColumn(
        "_partition_key", date_format("_timeslice", "yyyyMMdd").cast("integer")
    )

    dataflow.destination_df("raw.customer", df)
```

## Run an incremental load:

```python
# timeslice = Timeslice(2022, 7, 11)
timeslice = Timeslice(2022, 7, 12)
results = demo_landing_to_raw(
    timeslice = timeslice
)
```

## Run an bulk monthly load:

```python
timeslice = Timeslice(2022, 7, '*')
results = demo_landing_to_raw(
    timeslice = timeslice
)
```

## Run a full load:

```python
results = demo_landing_to_raw(
    timeslice = Timeslice(2022, '*', '*'),
    save_type = OverwriteSave
)
```

## Dependencies & Setup

This is a spark application with DeltaLake it requires following dependencies installed in order to run locally:
- [Java Runtime 11](https://openjdk.org/install/)
- [Apache Spark 3.3.0 hadoop3](https://spark.apache.org/downloads.html)

Ensure that the spark home path and is added to youy path is set Eg:
```
export SPARK_HOME="$HOME/opt/spark-3.3.0-bin-hadoop3"
```

Enable DeltaLake by:
```
cp $SPARK_HOME/conf/spark-defaults.conf.template  $SPARK_HOME/conf/spark-defaults.conf
```
Add the following to `spark-defaults.conf`:
```
spark.jars.packages               io.delta:delta-core_2.12:2.1.0
spark.sql.extensions              io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog   org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.sql.catalogImplementation   hive
```

## Python Project Setup

Create virual environment and install dependencies for local development:

```
python -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Running the Demo

Run `main.py`. If in vscode and the dependencies are setup you should just be able to hit F5 since .vscode is included in this repo.
When the execution completes, run `pyspark` in the project root and should should be able to query the result `raw.customer` table that was loaded.
Check the table properties and observer partitions that have been applied automatically by framework. The demo will also Zorder as per the configuration.
