from pyathena import connect
from datetime import datetime, date
import json
from botocore.config import Config

def get_workflow_runs(self, workflow_name):

    """
    This function retrieves and summarizes the details of the most recent execution (run) of an AWS Glue workflow,
    including its start and end times, duration, status (succeeded or failed), and any failed jobs within the workflow.
    """
   
    print(f'Checking last run for workflow: {workflow_name}...')
    
    try:
        workflow_details = self.glue_client.get_workflow_runs(Name=workflow_name
                                                                ,MaxResults=1
                                                                ,IncludeGraph=True
                                                                ).get('Runs',None)[0]
                
        started_on = workflow_details['StartedOn']
        completed_on = workflow_details['CompletedOn']
        workflow_runtime = completed_on - started_on

        # Retrieve the JobRunState for each glue job to determine the workflow run status as the workflow returns "COMPLETED" even when some glue jobs "FAIL"
        job_statuses = {
            job['JobDetails']['JobRuns'][0]['JobName']: job['JobDetails']['JobRuns'][0]['JobRunState']
            for job in workflow_details.get('Graph',None)['Nodes'] 
            if job['Type'] == 'JOB' and job['JobDetails'].get('JobRuns',None) is not None
        }

        workflow_run_status = 'SUCCEEDED' if all(status == 'SUCCEEDED' for job,status in job_statuses.items()) else 'FAILED'

        # Retrieve jobs that failed during the workflow run
        jobs_failed = ','.join([job.split('-')[-1] for job,status in job_statuses.items() if status == 'FAILED'])

        # Last workflow run relevant details
        return{
            'workflow_name': workflow_name,
            'workflow_run_id': workflow_details['WorkflowRunId'],
            'workflow_started_on': started_on.strftime("%Y-%m-%d %H:%M:%S"),
            'workflow_completed_on': completed_on.strftime("%Y-%m-%d %H:%M:%S"),
            'workflow_runtime_minutes': round(workflow_runtime.total_seconds() / 60,2),
            'workflow_run_status': workflow_run_status,
            'workflow_jobs_failed': jobs_failed
        }

    except:
        print(f'An error occurred while retrieving workflow run details for workflow: {workflow_name}!')
        return None


def get_workflow_metrics(self):

    """
    This function fetches metrics for multiple AWS Glue workflows based on specified data schemas.
    The resulting metrics are organized into a list and include a timestamp for when they were retrieved.
    """

    self.glue_client = self.connect_AWS_client("glue")
    data_schemas = ConfigCheck().data_schemas() #json file with data schemas to be checked

    if self.config['profile_name']:
        self.conn = connect(s3_staging_dir=self.config['s3_staging_dir'],
                            region_name=self.config['region_name'], profile_name=self.config['profile_name'])
    else:
        self.conn = connect(
            s3_staging_dir=self.config['s3_staging_dir'], region_name=self.config['region_name'])
    
    metrics = []

    for data_schema in data_schemas:
        workflow = data_schema.get('workflow_name', None)
        schema_dict = {'database': data_schema['database'],'schema': data_schema['schema']}
        workflow_details = self.get_workflow_runs(workflow)
        
        if workflow_details:
            metrics.append({**workflow_details, **schema_dict})
            for row in metrics:
                    row['lambda_timestamp'] = self.current_time.strftime('%Y-%m-%d %H:%M:%S')
            print(f'Workflow metrics retrieved for workflow {workflow}!')

    return metrics


def store_workflow_metrics(self, metrics):
    
    """
    This function stores workflow metrics as a JSON file on AWS S3, using a timestamped file name.
    If there are metrics available, it uploads them to the specified S3 bucket and folder; otherwise, it reports that there are no metrics to store.
    """

    print("Storing workflow metrics on AWS S3...")
    
    file_name = f'Last_Workflow_Run_{self.current_time.strftime("%Y%m%d-%H%M%S")}.json'
    folder = self.config['workflowlogs']
    s3_key = f'{folder}/{file_name}'
    bucket_name = self.config['bucket_name']
    s3 = self.connect_AWS_resource('s3')
    bucket = s3.Bucket(bucket_name)
    json_data = ''

    if metrics:
        for i in metrics:
            json_data += json.dumps(i, sort_keys=True) + '\n'
        r = bucket.put_object(Key=s3_key, Body=json_data)
        print('Workflow metrics stored in: ', r, len(json_data))
    
    else:
        print('No metrics to be stored!')