# Setup Data for Unit Testing

Let us make sure we have data setup in Databricks Environment.

* Download files to local file system.

```shell script
cd Downloads
wget https://data.gharchive.org/2021-01-13-0.json.gz
wget https://data.gharchive.org/2021-01-14-0.json.gz
wget https://data.gharchive.org/2021-01-15-0.json.gz
```

* Create required folders in databricks mount point - **/mnt/itv-github-db**.
* Copy files using AWS CLI

```shell script
aws s3 cp 2021-01-13-0.json.gz s3://itv-github-db/dev/landing/ --profile itvgithubdb
aws s3 cp 2021-01-14-0.json.gz s3://itv-github-db/dev/landing/ --profile itvgithubdb
aws s3 cp 2021-01-15-0.json.gz s3://itv-github-db/dev/landing/ --profile itvgithubdb
```

* Validate using Databricks Notebook.

```shell script
%fs ls /mnt/itv-github-db/dev/landing/
```

* Make changes to **app.py** and run to validate.

```python
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
```