from pyspark.sql import SparkSession


def main():
    spark = SparkSession. \
        builder. \
        appName('GitHub Activity - Databricks'). \
        getOrCreate()
    spark.sql('SELECT current_date').show()


if __name__ == '__main__':
    main()
