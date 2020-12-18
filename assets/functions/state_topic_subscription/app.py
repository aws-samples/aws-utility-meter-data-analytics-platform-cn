import os
import boto3
import json

client = boto3.client('stepfunctions')


def trigger_state_machine():
    state_machine_arn = os.environ['ml_pipeline_trigger_state_machine']

    running_executions = client.list_executions(
        stateMachineArn=state_machine_arn,
        statusFilter='RUNNING',
        maxResults=10
    )

    if not running_executions['executions']:
        print('start new execution')
        boto3 \
            .client('stepfunctions') \
            .start_execution(stateMachineArn=state_machine_arn)
    else:
        print('state machine already running.')


def lambda_handler(event, context):
    glue_job_name= os.environ['glue_job_name']

    message = event['Records'][0]['Sns']['Message']
    detail = json.loads(message)['detail']

    # TODO move to SNS topic filter
    job_name = (detail['jobName'])
    job_sate = ((detail['state']))
    if job_name == glue_job_name and job_sate == 'SUCCEEDED':
        trigger_state_machine()
    else:
        print('receive job state change event: [{}][{}]'.format(job_name, job_sate))
