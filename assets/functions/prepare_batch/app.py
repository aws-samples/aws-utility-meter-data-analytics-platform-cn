'''
Input event payload expected to be in the following format:

{
"Batch_start": "MAC000001",
"Batch_end": "MAC000020",
"Data_start": "2013-06-01",
"Data_end": "2014-01-01",
"Forecast_period": 7
}

'''

import boto3, io, os
import json
import pandas as pd

from pyathena import connect

REGION = os.environ['AWS_REGION']


def write_upload_file(bucket, path, data):
    jsonBuffer = io.StringIO()
    for d in data:
        jsonBuffer.write(json.dumps(d))
        jsonBuffer.write('\n')

    boto3.Session().resource('s3').Bucket(bucket).Object(path).put(Body=jsonBuffer.getvalue())

def get_weather(connection, start, db_schema):
    weather_data = '''select date_parse(time,'%Y-%m-%d %H:%i:%s') as datetime, temperature,
                    apparenttemperature, humidity
                    from "{}".weather
                    where time >= '{}'
                    order by 1;
                    '''.format(db_schema, start)
    df_weather = pd.read_sql(weather_data, connection)
    df_weather = df_weather.set_index('datetime')
    return df_weather

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
    USE_WEATHER_DATA = os.environ['With_weather_data']

    BATCH_START = event['Batch_start']
    BATCH_END = event['Batch_end']
    DATA_START = event['Data_start']
    DATA_END = event['Data_end']
    FORECAST_PERIOD = event['Forecast_period']
    prediction_length = FORECAST_PERIOD * 24


    connection = connect(s3_staging_dir='s3://{}/'.format(ATHENA_OUTPUT_BUCKET), region_name=REGION)

    meter_samples = get_meters(connection, BATCH_START, BATCH_END, DB_SCHEMA)

    # test data
    q = '''select date_trunc('HOUR', reading_date_time) as datetime, meter_id, sum(reading_value) as consumption
              from "{}".daily
              where meter_id in ('{}')
              and reading_date_time >= timestamp '{}'
              and reading_date_time < timestamp '{}'
              group by 2, 1
              ;'''.format(DB_SCHEMA, "','".join(meter_samples), DATA_START, DATA_END)

    batch_job = pd.read_sql(q, connection)
    batch_job = batch_job.set_index('datetime')
    print("read meter data from data lake")

    batchseries = {}
    for meter_id in meter_samples:
        data_kw = batch_job[batch_job['meter_id'] == meter_id].resample('1H').sum()
        batchseries[meter_id] = data_kw.iloc[:,0]  #np.trim_zeros(data_kw.iloc[:,0], trim='f')

    freq = 'H'
    start_dataset = pd.Timestamp(DATA_START, freq=freq)
    end_training = pd.Timestamp(DATA_END, freq=freq) - pd.Timedelta(1, unit='H')
    end_prediction = end_training + pd.Timedelta(prediction_length, unit='H')

    if USE_WEATHER_DATA == 1:
        df_weather = get_weather(connection, DATA_START, DB_SCHEMA)
        batch_data = [
            {
                "start": str(start_dataset),
                "target": ts[start_dataset:end_training - pd.Timedelta(1, unit='H')].tolist(),  # We use -1, because pandas indexing includes the upper bound
                "dynamic_feat": [df_weather['temperature'][start_dataset:end_prediction].tolist(),
                                 df_weather['humidity'][start_dataset:end_prediction].tolist(),
                                 df_weather['apparenttemperature'][start_dataset:end_prediction].tolist()]
            }
            for meterid, ts in batchseries.items()
        ]
    else:
        batch_data = [
            {
                "start": str(start_dataset),
                "target": ts[start_dataset:end_training - pd.Timedelta(1, unit='H')].tolist()  # We use -1, because pandas indexing includes the upper bound
            }
            for meterid, ts in batchseries.items()
        ]

    write_upload_file(S3_BUCKET, 'meteranalytics/input/batch_{}_{}/batch.json'.format(BATCH_START, BATCH_END), batch_data)
    print('Finished preparing batch data from {} to {}'.format(BATCH_START, BATCH_END))
