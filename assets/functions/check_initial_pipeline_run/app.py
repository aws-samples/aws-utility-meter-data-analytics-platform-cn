import boto3, os, json
import uuid

from botocore.errorfactory import ClientError


def check_if_file_exist(bucket, key):
    s3_client = boto3.client('s3')
    file_exists = True
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError:
        file_exists = False
        pass

    return file_exists


def load_json_from_file(bucket, path):
    s3 = boto3.client('s3')
    data = s3.get_object(Bucket=bucket, Key=path)

    return json.load(data['Body'])


def lambda_handler(event, context):
    working_bucket = os.environ['Working_bucket']
    file_key = "meteranalytics/initial_pass"


    initial_pipeline_passed = check_if_file_exist(working_bucket, file_key)

    if initial_pipeline_passed:
        parameter = load_json_from_file(working_bucket, file_key)
    else:
        parameter = {
            "Data_start": "2013-06-01",
            "Data_end": "2013-12-01",
            "Forecast_period": 7,
            "Training_samples": 50,
            "Training_instance_type": "ml.c5.2xlarge",
            "Endpoint_instance_type": "ml.m5.xlarge",
            "Training_job_name": "training-job-{}".format(str(uuid.uuid4())),
            "ModelName": "ml-model-{}".format(str(uuid.uuid4())),
            "ML_endpoint_name": "ml-endpoint-{}".format(str(uuid.uuid4())),
            "Meter_start": 1,
            "Meter_end": 100,
            "Batch_size": 25
        }

    return {
        **parameter,
        **event, # will override parameter with same key, can be used to override default parameter from the outside
        "initial_pipeline_passed": initial_pipeline_passed
    }
