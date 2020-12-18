'''
Testing event
{
  "Data_start": "2013-06-01",
  "Data_end": "2014-01-01",
  "Meter_id": "MAC004534",
  "ML_endpoint_name": "ml-endpoint-0001"
}
'''

import boto3, os
import pandas as pd 
import numpy as np
import json
from pyathena import connect

REGION = os.environ['AWS_REGION']
WORKING_BUCKET = os.environ['WORKING_BUCKET']

s3 = boto3.client('s3')
    
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
    
def encode_request(ts, weather):
    instance = {
          "start": str(ts.index[0]),
          "target": [x if np.isfinite(x) else "NaN" for x in ts]    
        }
    if weather is not None:
        instance["dynamic_feat"] = [weather['temperature'].tolist(),
                                  weather['humidity'].tolist(),
                                  weather['apparenttemperature'].tolist()]

    configuration = {
        "num_samples": 100,
        "output_types": ["quantiles"] ,
        "quantiles": ["0.9"]
    }

    http_request_data = {
        "instances": [instance],
        "configuration": configuration
    }

    return json.dumps(http_request_data).encode('utf-8')

def decode_response(response, freq, prediction_time):
    predictions = json.loads(response.decode('utf-8'))['predictions'][0]
    prediction_length = len(next(iter(predictions['quantiles'].values())))
    prediction_index = pd.date_range(start=prediction_time, end=prediction_time + pd.Timedelta(prediction_length-1, unit='H'), freq=freq)
    dict_of_samples = {}
    return pd.DataFrame(data={**predictions['quantiles'], **dict_of_samples}, index=prediction_index)

def load_json_from_file(bucket, path):
    data = s3.get_object(Bucket=bucket, Key=path)

    return json.load(data['Body'])

# expect request: forecast/{meter_id}?ml_endpoint_name={}&data_start={}&data_end={}
def lambda_handler(event, context):
    ATHENA_OUTPUT_BUCKET = os.environ['Athena_bucket']
    DB_SCHEMA = os.environ['Db_schema']
    USE_WEATHER_DATA = os.environ['With_weather_data']

    pathParameter = event["pathParameters"]
    queryParameter = event["queryStringParameters"]

    if ("meter_id" not in pathParameter) \
            or ("data_start" not in queryParameter) \
            or ("data_end" not in queryParameter):
        return {
            'statusCode': 500,
            'body': "error: meter_id, data_start, and data_end needs to be provided."
        }

    METER_ID = pathParameter['meter_id']
    ML_ENDPOINT_NAME = load_json_from_file(WORKING_BUCKET, "meteranalytics/initial_pass")["ML_endpoint_name"]
    DATA_START = queryParameter['data_start'].replace("-", "")
    DATA_END = queryParameter['data_end'].replace("-", "")

    connection = connect(s3_staging_dir='s3://{}/'.format(ATHENA_OUTPUT_BUCKET), region_name=REGION)
    query = '''select date_trunc('HOUR', reading_date_time) as datetime, sum(reading_value) as consumption
                from "{}".daily
                where meter_id = '{}' and date_str >= '{}'
                and  date_str < '{}'
                group by 1;
                '''.format(DB_SCHEMA, METER_ID, DATA_START, DATA_END)
    result = pd.read_sql(query, connection)
    result = result.set_index('datetime')

    data_kw = result.resample('1H').sum()
    timeseries = data_kw.iloc[:,0]  #np.trim_zeros(data_kw.iloc[:,0], trim='f')
    
    freq = 'H'
    df_weather = None
    if USE_WEATHER_DATA == 1:
        df_weather = get_weather(connection, DATA_START, DB_SCHEMA)

    runtime= boto3.client('runtime.sagemaker')
    response = runtime.invoke_endpoint(EndpointName=ML_ENDPOINT_NAME,
                                       ContentType='application/json',
                                       Body=encode_request(timeseries[:], df_weather))
    prediction_time = timeseries.index[-1] + pd.Timedelta(1, unit='H')
    df_prediction = decode_response(response['Body'].read(), freq, prediction_time)

    df_prediction.columns = ['consumption']
    prediction_result = df_prediction.to_json()

    return {
        "statusCode": 200,
        "body": prediction_result
    }
