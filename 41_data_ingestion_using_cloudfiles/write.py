def write(df, tgt_dir, file_format, cp_dir):
    df.coalesce(10).writeStream.trigger(once=True) \
        .format(file_format) \
        .option("checkpointLocation", cp_dir) \
        .partitionBy('year', 'month', 'day') \
        .outputMode('overwrite') \
        .start(tgt_dir)
