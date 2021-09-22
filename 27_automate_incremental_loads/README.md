# Wrapper for Incremental Loads

Let us go ahead and develop wrapper for incremental loads using Databricks clusters.

* Install Databricks Client Library.

```shell script
pip install databricks-client
```

* Configure and Validate Databricks Client Library.
* We will get details of all the jobs we have deployed so far.
```python
import os
import databricks_client

client_uri = f"{os.environ.get('DATABRICKS_URI')}/api/2.0"
db_token = os.environ.get('DATABRICKS_TOKEN')
client = databricks_client.create(client_uri)
client.auth_pat_token(db_token)

client.ensure_available()
job_list = client.get('jobs/list')
for job in job_list["jobs"]:
    print(job)
```

* Here is how we can get the details of a given job.

```python
import os
import databricks_client

client_uri = f"{os.environ.get('DATABRICKS_URI')}/api/2.0"
db_token = os.environ.get('DATABRICKS_TOKEN')
client = databricks_client.create(client_uri)
client.auth_pat_token(db_token)

client.ensure_available()

job_id = 27
job_details = client.get(f'jobs/get?job_id={job_id}')

job_settings = job_details['settings']
```

* We can update the environment variable **SRC_FILE_PATTERN** using this approach.

```python
job_details['settings']['new_cluster']['spark_env_vars']['SRC_FILE_PATTERN'] = '2021-01-15'
```

* We can then run the job using **run-now** API.

```python
job_run_response = client.post('jobs/run-now', json=job_details)
print(job_run_response)
```

* We can then list the jobs that are currently running.

```python
job_runs = client.get(f'jobs/runs/list?job_id={job_id}&active_only=true')
print(job_runs)

job_run_details = client.get(f'jobs/runs/get?run_id={job_runs["runs"][0]["run_id"]}')
print(job_run_details)
```
