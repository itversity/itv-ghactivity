import os
import databricks_client


def main():
    client_uri = f"{os.environ.get('DATABRICKS_URI')}/api/2.0"
    db_token = os.environ.get('DATABRICKS_TOKEN')
    job_id = os.environ.get('JOB_ID')
    client = databricks_client.create(client_uri)
    client.auth_pat_token(db_token)

    client.ensure_available()

    job_details = client.get(f'jobs/get?job_id={job_id}')

    job_details['settings']['new_cluster']['spark_env_vars']['SRC_FILE_PATTERN'] = '2021-01-15'

    job_run_response = client.post('jobs/run-now', json=job_details)
