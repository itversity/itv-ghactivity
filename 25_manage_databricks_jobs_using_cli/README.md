# Manage Databricks Jobs using CLI

Let us understand how to manage Databricks Jobs using CLI.

* We can list the jobs and get the job details using below commands.

```shell script
databricks jobs list
databricks jobs get --job-id 27
```

* We can capture the output in the form of JSON. We need to capture the details of **settings** from json and use it to change the job settings.
* We can change the value for **SRC_FILE_PATTERN** to submit the job for next day.

```json
{
  "name": "GHActivity",
  "new_cluster": {
    "spark_version": "7.5.x-scala2.12",
    "aws_attributes": {
      "zone_id": "us-east-1e",
      "first_on_demand": 1,
      "availability": "SPOT_WITH_FALLBACK",
      "instance_profile_arn": "arn:aws:iam::582845781536:instance-profile/ITVGitHubDatabricksGlueCatalogRole",
      "spot_bid_price_percent": 100,
      "ebs_volume_count": 0
    },
    "node_type_id": "i3.xlarge",
    "spark_env_vars": {
      "TGT_FILE_FORMAT": "parquet",
      "SRC_DIR": "dbfs:/mnt/itv-github-db/prod/landing/ghactivity/",
      "SRC_FILE_PATTERN": "2021-01-14",
      "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
      "SRC_FILE_FORMAT": "json",
      "TGT_DIR": "dbfs:/mnt/itv-github-db/prod/raw/ghactivity/",
      "ENVIRON": "DATABRICKS"
    },
    "enable_elastic_disk": false,
    "num_workers": 8
  },
  "email_notifications": {},
  "timeout_seconds": 0,
  "spark_submit_task": {
    "parameters": [
      "--py-files",
      "dbfs:/jobs/itv-ghactivity/itv-ghactivity.zip",
      "dbfs:/jobs/itv-ghactivity/app.py"
    ]
  },
  "max_concurrent_runs": 1
}
```

* We can reset the job and run using **reset** and **run-now**.

```shell script
databricks jobs reset --job-id 27 --json-file job.json
databricks jobs run-now --job-id 27
```

* We can list the current running jobs and also get the job run details using **databricks runs**.

```shell script
databricks runs list --active-only
databricks runs get --run-id 105
```