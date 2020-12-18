import sys, boto3, json, datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *


def load_state_information():
    """
        reads state information
    :return:
    """
    s3 = boto3.resource('s3', region_name=args['region'])
    state_information_file = s3.Object(args['temp_workflow_bucket'], 'glue_workflow_distinct_dates')
    return json.load(state_information_file.get()['Body'])


def calculate_dates_of_month(date_str):
    """
    Calculates all dates in the month of the given date.

    20200824 --> Month 8 -- dates in month:
    ['20200801', '20200802', '20200803', '20200804', ...,  '20200829', '20200830', '20200831']

    :param date_str:
    :return: List of dates str yyyyMMdd
    """
    date_time_obj = datetime.datetime.strptime(date_str, '%Y%m%d')

    month = date_time_obj.month
    year = date_time_obj.year

    days_of_month = (datetime.date(year, month + 1, 1) - datetime.date(year, month, 1)).days
    first_date_of_month = datetime.date(year, month, 1)
    last_date_of_month = datetime.date(year, month, days_of_month)
    delta = last_date_of_month - first_date_of_month

    return "({})".format(",".join((first_date_of_month + datetime.timedelta(days=i)).strftime('%Y%m%d') for i in range(delta.days + 1)))


def aggregate_and_write_data_to_s3(bucket_path, push_down_predicate=""):
    """
        If provided, takes a push down predicate to select exactly the data that are needed to be aggregated.
        Otherwise the whole data set gets aggregated

    :param bucket_path:
    :param push_down_predicate:
    """
    meter_data_to_aggregate = glueContext.create_dynamic_frame.from_catalog(database=args['db_name'], \
                                                                            table_name="daily", \
                                                                            transformation_ctx="meter_data_to_aggregate", \
                                                                            push_down_predicate=push_down_predicate)

    daily_aggregated_interval_reads = meter_data_to_aggregate.toDF() \
        .groupby('meter_id', 'month', 'year') \
        .agg(sum("reading_value").alias("aggregated_consumption"))

    daily_aggregated_interval_reads \
        .repartition("year", "month") \
        .write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("year", "month") \
        .parquet(bucket_path)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'db_name', 'business_zone_bucket', 'temp_workflow_bucket', 'region'])

sc = SparkContext()
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3."+args['region']+".amazonaws.com.cn")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

business_zone_bucket_path = "s3a://" + args['business_zone_bucket'] + "/aggregated/monthly"

daily_meter_reads = glueContext.create_dynamic_frame.from_catalog(database=args['db_name'], table_name="daily",
                                                                  transformation_ctx="cleanedMeterDataSource")

state_information = load_state_information()

if state_information["first_run"]:
    aggregate_and_write_data_to_s3(business_zone_bucket_path)
else:
    dates_to_process = state_information["dates"]
    if dates_to_process:
        for date in dates_to_process:
            dates_in_month = calculate_dates_of_month(date)
            aggregate_and_write_data_to_s3(business_zone_bucket_path,
                                           "(reading_type == 'INT' and date_str IN '{}')".format(dates_in_month))

job.commit()
