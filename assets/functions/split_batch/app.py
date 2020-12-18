'''
sample lambda input
{
    "Meter_start": 1,
    "Meter_end": 100,
    "Batch_size": 20
}
'''

import uuid, os
import pandas as pd

from pyathena import connect

REGION = os.environ['AWS_REGION']


def lambda_handler(event, context):
    #list index starts from 0
    start       = event['Meter_start'] - 1
    end         = event['Meter_end'] - 1
    batchsize   = event['Batch_size']
    athena_bucket = os.environ['Athena_bucket']
    s3_bucket   = os.environ['Working_bucket']
    schema = os.environ['Db_schema']

    connection = connect(s3_staging_dir='s3://{}/'.format(athena_bucket), region_name=REGION)

    # Todo, more efficient way is to create a meter list table instead of getting it from raw data
    df_meters = pd.read_sql('''select distinct meter_id from "{}".daily order by meter_id'''.format(schema), connection)
    meters = df_meters['meter_id'].tolist()

    id = uuid.uuid4().hex
    batchdetail = []

    # Cap the batch size to 100 so the lambda function doesn't timeout
    if batchsize > 100:
        batchsize = 100
    for a in range(start, min(end, len(meters)), batchsize):
        job = {}
        meter_start = meters[a]
        meter_end = meters[min(end-1, a+batchsize-1)]
        # Sagemaker transform job name cannot be more than 64 characters.
        job['Batch_job'] = 'job-{}-{}-{}'.format(id, meter_start, meter_end)
        job['Batch_start'] = meter_start
        job['Batch_end'] = meter_end
        job['Batch_input'] = 's3://{}/meteranalytics/input/batch_{}_{}'.format(s3_bucket, meter_start, meter_end)
        job['Batch_output'] = 's3://{}/meteranalytics/inference/batch_{}_{}'.format(s3_bucket, meter_start, meter_end)
        batchdetail.append(job)

    return batchdetail