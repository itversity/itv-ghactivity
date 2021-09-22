import os, logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, \
    month, dayofmonth


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


def from_cloud_files(spark, data_dir, file_format):
    schema = spark.read.json(f"{data_dir}2021-01-13-0.json.gz").schema
    df = spark.readStream.format(file_format) \
        .schema(schema) \
        .load(data_dir)
    return df


def transform(df):
    return df.withColumn('year', year('created_at')). \
        withColumn('month', month('created_at')). \
        withColumn('day', dayofmonth('created_at'))


def write(df, tgt_dir, file_format, cp_dir):
    df.coalesce(10).writeStream.trigger(once=True) \
        .format(file_format) \
        .option("checkpointLocation", cp_dir) \
        .partitionBy('year', 'month', 'day') \
        .outputMode("append") \
        .start(tgt_dir)


def main():
    env = os.environ.get('ENVIRON')
    src_dir = os.environ.get('SRC_DIR')
    src_file_format = os.environ.get('SRC_FILE_FORMAT')
    tgt_dir = os.environ.get('TGT_DIR')
    tgt_file_format = os.environ.get('TGT_FILE_FORMAT')
    cp_dir = os.environ.get('CP_DIR')
    spark = get_spark_session(env, 'GitHub Activity - Cloud Files')
    logging.info(src_dir)
    df = from_cloud_files(spark, src_dir, src_file_format)
    df_processed = transform(df)
    write(df_processed, tgt_dir, tgt_file_format, cp_dir)


if __name__ == '__main__':
    main()
