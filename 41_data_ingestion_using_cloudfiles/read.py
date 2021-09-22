import logging


def from_cloud_files(spark, data_dir, file_format):
    schema = spark.read.json(data_dir+'2021-01-13-0.json.gz').schema
    logging.info(schema)
    df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", file_format) \
        .schema(schema) \
        .load(data_dir)
    return df
