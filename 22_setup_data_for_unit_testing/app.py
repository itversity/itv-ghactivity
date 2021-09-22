from pyspark.sql import SparkSession


def main():
    spark = SparkSession. \
        builder. \
        appName('Getting Started - Databricks'). \
        getOrCreate()
    count = spark. \
        read. \
        json('dbfs:/mnt/itv-github-db/dev/landing/2021-01-13-0.json.gz'). \
        count()
    print(count)


if __name__ == '__main__':
    main()
