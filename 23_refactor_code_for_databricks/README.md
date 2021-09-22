# Refactor Code for Databricks

Let us make necessary changes to the code so that we can run on Databricks Clusters.
* Update **util.py** to use run on Databricks clusters.

```python
from pyspark.sql import SparkSession


def get_spark_session(env, app_name):
    if env == 'DEV':
        spark = SparkSession. \
            builder. \
            master('local'). \
            appName(app_name). \
            getOrCreate()
        return spark
    elif env == 'PROD':
        spark = SparkSession. \
            builder. \
            master('yarn'). \
            appName(app_name). \
            getOrCreate()
        return spark
    elif env == 'DATABRICKS':
        spark = SparkSession. \
            builder. \
            appName(app_name). \
            getOrCreate()
        return spark
    return
```

* We need to revert back the code in **app.py** to what we have earlier.

```python
import os
from util import get_spark_session
from read import from_files
from write import write
from process import transform


def main():
    env = os.environ.get('ENVIRON')
    src_dir = os.environ.get('SRC_DIR')
    file_pattern = f"{os.environ.get('SRC_FILE_PATTERN')}-*"
    src_file_format = os.environ.get('SRC_FILE_FORMAT')
    tgt_dir = os.environ.get('TGT_DIR')
    tgt_file_format = os.environ.get('TGT_FILE_FORMAT')
    spark = get_spark_session(env, 'GitHub Activity - Reading and Writing Data')
    df = from_files(spark, src_dir, file_pattern, src_file_format)
    df_transformed = transform(df)
    write(df_transformed, tgt_dir, tgt_file_format)


if __name__ == '__main__':
    main()
```

* Run the application after adding environment variables. Validate for one day.
  * 2021-01-13
* Update the following environment variables. Make sure the source and target paths are pointing to dev directories created as part of DBFS.

```shell script
ENVIRON=DATABRICKS
SRC_DIR=dbfs:/mnt/itv-github-db/dev/landing/ghactivity/
SRC_FILE_FORMAT=json
SRC_FILE_PATTERN=2021-01-13
TGT_DIR=dbfs:/mnt/itv-github-db/dev/raw/ghactivity/
TGT_FILE_FORMAT=parquet
```

* Check for files in the target location by running this as part of Databricks Notebook. 

```shell script
aws s3 ls s3://itv-github-db/dev/raw --recursive --profile itvgithubdb
```

* We can also run this code to validate the data.

```python
from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    appName('GitHub Activity Validation'). \
    getOrCreate()

# You can run this code as part of Databricks Notebook without creating Spark Session object.
tgt_file_path = 'dbfs:/mnt/itv-github-db/dev/raw/ghactivity'
tgt_df = spark.read.parquet(tgt_file_path)
tgt_df.printSchema()
tgt_df.select('repo.*').show()
tgt_df.count()

from pyspark.sql.functions import col, to_date
tgt_df. \
  groupBy(to_date(col('created_at')).alias('created_at')). \
  count(). \
  show()
```

