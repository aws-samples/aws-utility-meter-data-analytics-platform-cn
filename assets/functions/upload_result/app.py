'''
Input event payload expected to be in the following format:

{
"Batch_start": "MAC000001",
"Batch_end": "MAC000010",
"Data_start": "2013-06-01",
"Data_end": "2014-01-01",
"Forecast_period": 7
}

'''

import boto3, os
import json
import pandas as pd
import numpy as np

from pyathena import connect

REGION = os.environ['AWS_REGION']


def get_meters(connection, start, end, db_schema):
    selected_households = '''select distinct meter_id
                  from "{}".daily where meter_id between '{}' and '{}' order by meter_id;
                  '''.format(db_schema, start, end)

    df_meters = pd.read_sql(selected_households, connection)
    return df_meters['meter_id'].tolist()

def lambda_handler(event, context):
    ATHENA_OUTPUT_BUCKET = os.environ['Athena_bucket']
    S3_BUCKET = os.environ['Working_bucket']
    DB_SCHEMA = os.environ['Db_schema']

    BATCH_START = event['Batch_start']
    BATCH_END = event['Batch_end']
    FORECAST_START = event['Data_end']
    FORECAST_PERIOD = event['Forecast_period']
    prediction_length = FORECAST_PERIOD * 24

    connection = connect(s3_staging_dir='s3://{}/'.format(ATHENA_OUTPUT_BUCKET), region_name=REGION)

    output = 'meteranalytics/inference/batch_%s_%s/batch.json.out' % (BATCH_START, BATCH_END)
    boto3.Session().resource('s3').Bucket(S3_BUCKET).Object(output).download_file('/tmp/batch.out.json')
    print('get inference result')

    freq = 'H'
    prediction_time = pd.Timestamp(FORECAST_START, freq=freq)
    prediction_index = pd.date_range(start=prediction_time, end=prediction_time+pd.Timedelta(prediction_length-1, unit='H'), freq=freq)
    dict_of_samples = {}

    meterids = get_meters(connection, BATCH_START, BATCH_END, DB_SCHEMA)

    results = pd.DataFrame(columns = ['meterid', 'datetime', 'kwh'])
    i = 0
    with open('/tmp/batch.out.json') as fp:
        for line in fp:
            df = pd.DataFrame(data={**json.loads(line)['quantiles'],
                                    **dict_of_samples}, index=prediction_index)
            dataframe=pd.DataFrame({'meter_id': np.array([meterids[i] for x in range(df['0.9'].count())]),
                                    'datetime':df.index.values,
                                    'consumption':df['0.9'].values})
            i = i+1
            results = results.append(dataframe)

    results.to_csv('/tmp/forecast.csv', index=False)
    boto3.Session().resource('s3').Bucket(S3_BUCKET).Object(os.path.join('meteranalytics', 'forecast/{}/batch_{}_{}.csv'.format(FORECAST_START, BATCH_START, BATCH_END))).upload_file('/tmp/forecast.csv')
