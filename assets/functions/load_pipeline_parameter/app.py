""" Loads the pipeline parameters from the initial_pass file and passes them back.
    Parameter that gets passed to this function will overwrite the parameter stored in the inital_pass file """

import json, boto3, os


def load_json_from_file(bucket, path):
    s3 = boto3.client('s3')
    data = s3.get_object(Bucket=bucket, Key=path)

    return json.load(data['Body'])


def lambda_handler(event, context):
    working_bucket = os.environ['Working_bucket']

    parameter = load_json_from_file(working_bucket, "meteranalytics/initial_pass")

    return {
        **parameter,
        **event
    }
