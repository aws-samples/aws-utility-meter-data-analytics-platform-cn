'''
Input event payload expected to be in the following format:

{
  "Data_start": "2013-06-01",
  "Data_end": "2014-01-01",
  "Forecast_period": 7,
  "Training_samples": 50
}

'''

import boto3, os, io
import json
import pandas as pd

from pyathena import connect

REGION = os.environ['AWS_REGION']

def get_weather(connection, start, schema):
    weather_data = '''select date_parse(time,'%Y-%m-%d %H:%i:%s') as datetime, 
    temperature, apparenttemperature,  humidity
    from "{}".weather
    where time >= '{}'
    order by 1;
    '''.format(schema, start)
    df_weather = pd.read_sql(weather_data, connection)
    df_weather = df_weather.set_index('datetime')
    return df_weather

def get_meters(connection, samples, schema):
    selected_households = '''select distinct meter_id
        from "{}".daily limit {};'''.format(schema, samples)

    df_meters = pd.read_sql(selected_households, connection)
    return df_meters['meter_id'].tolist()

def write_upload_file(bucket, path, data):
    jsonBuffer = io.StringIO()
    for d in data:
        jsonBuffer.write(json.dumps(d))
        jsonBuffer.write('\n')

    boto3.Session().resource('s3').Bucket(bucket).Object(path).put(Body=jsonBuffer.getvalue())

def write_json_to_file(bucket, path, data):
    boto3.Session().resource('s3').Bucket(bucket).Object(path).put(Body=json.dumps(data))

def lambda_handler(event, context):
    ATHENA_OUTPUT_BUCKET = os.environ['Athena_bucket']
    S3_BUCKET = os.environ['Working_bucket']
    DB_SCHEMA = os.environ['Db_schema']
    USE_WEATHER_DATA = os.environ['With_weather_data']

    TRAINING_SAMPLES = event['Training_samples']
    DATA_START = event['Data_start']
    DATA_END = event['Data_end']
    FORECAST_PERIOD = event['Forecast_period']
    prediction_length = FORECAST_PERIOD * 24

    connection = connect(s3_staging_dir='s3://{}/'.format(ATHENA_OUTPUT_BUCKET), region_name=REGION)

    meter_samples = get_meters(connection, TRAINING_SAMPLES, DB_SCHEMA)

    q = '''
            select date_trunc('HOUR', reading_date_time) as datetime, meter_id, sum(reading_value) as consumption
                from "{}".daily
                where meter_id in ('{}')
                and reading_date_time >= timestamp '{}'
                and reading_date_time < timestamp '{}'
                group by 2, 1
        '''.format(DB_SCHEMA, "','".join(meter_samples), DATA_START, DATA_END)

    result = pd.read_sql(q, connection)
    result = result.set_index('datetime')

    timeseries = {}
    for meter_id in meter_samples:
        data_kw = result[result['meter_id'] == meter_id].resample('1H').sum()
        timeseries[meter_id] = data_kw.iloc[:,0]  #np.trim_zeros(data_kw.iloc[:,0], trim='f')

    freq = 'H'
    num_test_windows = 2
    start_dataset = pd.Timestamp(DATA_START, freq=freq)
    end_dataset = pd.Timestamp(DATA_END, freq=freq) - pd.Timedelta(1, unit='H')
    end_training = end_dataset - pd.Timedelta(prediction_length * num_test_windows, unit='H')

    if USE_WEATHER_DATA == 1:
        df_weather = get_weather(connection, DATA_START, DB_SCHEMA)

        training_data = [
            {
                "start": str(start_dataset),
                "target": ts[start_dataset:end_training].tolist(),  # We use -1, because pandas indexing includes the upper bound
                "dynamic_feat": [df_weather['temperature'][start_dataset:start_dataset + pd.Timedelta(ts[start_dataset:end_training].size-1, unit='H')].tolist(),
                                 df_weather['humidity'][start_dataset:start_dataset + pd.Timedelta(ts[start_dataset:end_training].size-1, unit='H')].tolist(),
                                 df_weather['apparenttemperature'][start_dataset:start_dataset + pd.Timedelta(ts[start_dataset:end_training].size-1, unit='H')].tolist()]
            }
            for meterid, ts in timeseries.items()
        ]

        # there could be missing data, so use actual timeseries size
        testing_data = [
            {
                "start": str(start_dataset),
                "target": ts[start_dataset:end_dataset].tolist(),
                "dynamic_feat": [df_weather['temperature'][start_dataset:start_dataset + pd.Timedelta(ts[start_dataset:end_dataset].size-1, unit='H')].tolist(),
                                 df_weather['humidity'][start_dataset:start_dataset + pd.Timedelta(ts[start_dataset:end_dataset].size-1, unit='H')].tolist(),
                                 df_weather['apparenttemperature'][start_dataset:start_dataset + pd.Timedelta(ts[start_dataset:end_dataset].size-1, unit='H')].tolist()]
            }
            for k in range(1, num_test_windows + 1)
            for meterid, ts in timeseries.items()
        ]
    else:
        training_data = [
            {
                "start": str(start_dataset),
                "target": ts[start_dataset:end_training].tolist()  # We use -1, because pandas indexing includes the upper bound
            }
            for meterid, ts in timeseries.items()
        ]

        testing_data = [
            {
                "start": str(start_dataset),
                "target": ts[start_dataset:end_dataset].tolist()
            }
            for k in range(1, num_test_windows + 1)
            for meterid, ts in timeseries.items()
        ]

    write_upload_file(S3_BUCKET, 'meteranalytics/train/training.json', training_data)
    write_upload_file(S3_BUCKET, 'meteranalytics/test/testing.json', testing_data)

    write_json_to_file(S3_BUCKET, 'meteranalytics/initial_pass', event)
