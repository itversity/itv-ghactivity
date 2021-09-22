# Deploy as Databricks Job

Let us setup the data and deploy the job as part of the Databricks environment using Databricks Web Interface.

* Here are the commands to download the files.

```shell script
mkdir -p ~/Downloads/ghactivity
cd ~/Downloads/ghactivity

wget https://data.gharchive.org/2021-01-13-{0..23}.json.gz
wget https://data.gharchive.org/2021-01-14-{0..23}.json.gz
wget https://data.gharchive.org/2021-01-15-{0..23}.json.gz
```

* Copy the files into HDFS landing folders.

```shell script

nohup aws s3 \
  cp . s3://itv-github-db/prod/landing/ghactivity/ \
  --exclude "*" \
  --include "2021-01-13*" \
  --recursive \
  --profile itvgithubdb &

nohup aws s3 \
  cp . s3://itv-github-db/prod/landing/ghactivity/ \
  --exclude "*" \
  --include "2021-01-14*" \
  --recursive \
  --profile itvgithubdb &

nohup aws s3 \
  cp . s3://itv-github-db/prod/landing/ghactivity/ \
  --exclude "*" \
  --include "2021-01-15*" \
  --recursive \
  --profile itvgithubdb &

# Validating Files in AWS S3 bucket
aws s3 \
  ls s3://itv-github-db/prod/landing/ghactivity/ \
  --profile itvgithubdb

aws s3 \
  ls s3://itv-github-db/prod/landing/ghactivity/ \
  --profile itvgithubdb|wc -l
```

* Get the size of the files programmatically.

```python
from pyspark.dbutils import DBUtils
dbutils = DBUtils()
path = 'dbfs:/mnt/itv-github-db/prod/landing/ghactivity/'

size = 0
for file in dbutils.fs.ls(path): 
    size += file.size

print(size)
```

* Let us go ahead and build the code bundle to deploy as part of Databricks environment.

```shell script
zip -r itv-ghactivity.zip read.py process.py write.py util.py
```

* As the files are successfully copied, now we need to ensure that code bundle is deployed in DBFS.

```shell script
databricks fs mkdirs dbfs:/jobs
databricks fs mkdirs dbfs:/jobs/itv-ghactivity/
databricks fs cp itv-ghactivity.zip dbfs:/jobs/itv-ghactivity/
databricks fs cp app.py dbfs:/jobs/itv-ghactivity/
```
* Run the application after adding environment variables. Validate for multiple days.
  * 2021-01-13
  * 2021-01-14
  * 2021-01-15
* Here are the environment variables that need to be set using console.

```shell script
ENVIRON=DATABRICKS
SRC_DIR=dbfs:/mnt/itv-github-db/prod/landing/ghactivity/
SRC_FILE_FORMAT=json
TGT_DIR=dbfs:/mnt/itv-github-db/prod/raw/ghactivity/
TGT_FILE_FORMAT=parquet
```

* We will set **SRC_FILE_PATTERN** to appropriate value for each run.

```shell script
SRC_FILE_PATTERN=2021-01-13
```
* Check for files in the target location. 

```shell script
aws s3 \
  ls s3://itv-github-db/prod/raw/ghactivity/ \
  --profile itvgithubdb \
  --recursive
```

* We will now submit the jobs using command line to process data related to 2021-01-14 and 2021-01-15 after which we will run the detailed validations against the data processed.
