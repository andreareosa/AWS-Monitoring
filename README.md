# AWS Monitoring: infrastructure monitoring helper functions

## Introduction

This GitHub repository offers a collection of custom-built Python functions designed to simplify AWS infrastructure monitoring tasks. Whether you need to retrieve workflow metrics, check the status of AWS Glue jobs, or store monitoring data in AWS S3, these functions provide a convenient toolkit to enhance your cloud monitoring capabilities.

## Table of Contents

- [Retrieve and store workflow runs](#retrieve-and-store-workflow-runs)

## Retrieve and store workflow runs

**[workflow-runs.py](https://github.com/andreareosa/AWS-Monitoring/blob/main/workflow-runs.py)** This Python script comprises three essential functions, each designed to streamline various aspects of AWS Glue workflow monitoring:

* get_workflow_runs: With 'get_workflow_runs,' you can retrieve relevant details about the latest execution of an AWS Glue workflow. This function offers insights into start and end times, runtime duration, workflow status (success or failure), and provides a breakdown of any failed jobs within the workflow.

* get_workflow_metrics: 'get_workflow_metrics' allows you to gather comprehensive metrics for multiple AWS Glue workflows, all based on specified data schemas. It collects workflow specifics, including execution statistics and statuses, and pairs them with associated database and schema data. The result is a organized list of metrics, complete with a timestamp indicating when the data was retrieved.

* store_workflow_metrics: This function simplifies the process of storing AWS Glue workflow metrics by saving them as a JSON file on an AWS S3 bucket. It generates a unique file name, timestamped for your convenience, converts the metrics into JSON format, and efficiently uploads the data to your specified S3 location. Should metrics be available, this function executes the storage process and provides comprehensive details; in cases where no metrics are present, it gracefully communicates the absence.

## Send workflow run metrics from AWS to MS Teams

**[postTeams.py](https://github.com/andreareosa/AWS-Monitoring/blob/main/postTeams.py)** This function was created for AWS ETL monitoring system and it sends the summary from the last workflow execution from AWS Glue when ingesting data to AWS.

* The function builds the message payload to be sent to MS Teams using metrics from get_workflow_metrics function and the content of this payload depends on the workflow's status, whether it succeeded or failed. The notifications are sent as message cards and the JSON format used is described here.
* To make the HTTP POST request we use urllib3 library instead of requests library because it's currently not supported on AWS Lambda and therefore can't be imported directly (at least on python3.8 and above)
