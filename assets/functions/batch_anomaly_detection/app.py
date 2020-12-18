import boto3, os
import pandas as pd

from pyathena import connect
from fbprophet import Prophet

REGION = os.environ['AWS_REGION']

def weekend(ds):
    ds = pd.to_datetime(ds)

    if ds.weekday() > 4:
        return 1
    else:
        return 0
def get_batch_data(meter_start, meter_end, data_end, db_schema, connection):
    query = '''select meter_id, date_trunc('day', reading_date_time) as ds, sum(reading_value) as y 
  from "{}".daily
    where meter_id between '{}' and '{}'
  and reading_date_time > date_add('year', -1, timestamp '{}')
  and reading_date_time <= timestamp '{}'
    group by 2,1
    order by 2,1;
    '''.format(db_schema,meter_start, meter_end, data_end, data_end)

    df_daily = pd.read_sql(query, connection)
    df_daily['weekend'] = df_daily['ds'].apply(weekend)
    return df_daily

def fit_predict_model(meter, timeseries):
    m = Prophet(daily_seasonality = False, yearly_seasonality = True, weekly_seasonality = True,
                seasonality_mode = 'multiplicative',
                interval_width = .98,
                changepoint_range = .8)
    m.add_country_holidays(country_name='UK')
    m.add_regressor('weekend')

    m = m.fit(timeseries)
    forecast = m.predict(timeseries)
    forecast['consumption'] = timeseries['y'].reset_index(drop = True)
    forecast['meter_id'] = meter

    forecast['anomaly'] = 0
    forecast.loc[forecast['consumption'] > forecast['yhat_upper'], 'anomaly'] = 1
    forecast.loc[forecast['consumption'] < forecast['yhat_lower'], 'anomaly'] = -1

    #anomaly importances
    forecast['importance'] = 0
    forecast.loc[forecast['anomaly'] ==1, 'importance'] = \
        (forecast['consumption'] - forecast['yhat_upper'])/forecast['consumption']
    forecast.loc[forecast['anomaly'] ==-1, 'importance'] = \
        (forecast['yhat_lower'] - forecast['consumption'])/forecast['consumption']

    return forecast


def process_batch(meter_start, meter_end, data_end, db_schema, connection):
    query = '''select meter_id, max(ds) as ds from "{}".anomaly
               where meter_id between '{}' and '{}' group by 1;
        '''.format(db_schema, meter_start, meter_end)
    df_anomaly = pd.read_sql(query, connection)
    anomaly_meters = df_anomaly.meter_id.tolist()

    df_timeseries = get_batch_data(meter_start, meter_end, data_end, db_schema, connection)
    meters = df_timeseries.meter_id.unique()
    column_list=['meter_id', 'ds', 'consumption', 'yhat_lower', 'yhat_upper', 'anomaly', 'importance']
    df_result = pd.DataFrame(columns=column_list)
    for meter in meters:
        df_meter = df_timeseries[df_timeseries.meter_id == meter]
        # Run anomaly detection only if it's not done before or there are new data added
        if meter not in anomaly_meters:
            print("process anomaly for meter", meter)
            df_forecast = fit_predict_model(meter, df_meter)
            df_result = df_result.append(df_forecast[column_list], ignore_index = True)
        else:
            latest = pd.to_datetime(df_anomaly[df_anomaly.meter_id == meter]['ds'].iloc[0])
            if df_meter.ds.max() > latest:
                print("process anomaly for meter {} from {}".format(meter, latest))
                df_forecast = fit_predict_model(meter, df_meter)
                df_new_anomaly = df_forecast[df_forecast.ds > latest]
                df_result = df_result.append(df_new_anomaly[column_list], ignore_index = True)
            else:
                print("skip meter", meter)

    return df_result

def lambda_handler(event, context):
    ATHENA_OUTPUT_BUCKET = os.environ['Athena_bucket']
    S3_BUCKET = os.environ['Working_bucket']
    DB_SCHEMA = os.environ['Db_schema']

    BATCH_START = event['Batch_start']
    BATCH_END = event['Batch_end']
    DATA_END = event['Data_end']

    connection = connect(s3_staging_dir='s3://{}/'.format(ATHENA_OUTPUT_BUCKET), region_name=REGION)
    result = process_batch(BATCH_START, BATCH_END, DATA_END, DB_SCHEMA, connection)

    result.to_csv('/tmp/anomaly.csv', index=False)
    boto3.Session().resource('s3').Bucket(S3_BUCKET).Object(os.path.join('meteranalytics', 'anomaly/{}/batch_{}_{}.csv'.format(DATA_END,BATCH_START, BATCH_END))).upload_file('/tmp/anomaly.csv')

