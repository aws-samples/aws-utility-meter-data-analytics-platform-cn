import sys, boto3, json

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
        .groupby('meter_id', 'date_str') \
        .agg(sum("reading_value").alias("aggregated_consumption"))

    daily_aggregated_interval_reads \
        .repartition("date_str") \
        .write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("date_str") \
        .parquet(bucket_path)


## @params: [JOB_NAME, db_name, business_zone_bucket, temp_workflow_bucket]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'db_name', 'business_zone_bucket', 'temp_workflow_bucket', 'region'])

sc = SparkContext()
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3."+args['region']+".amazonaws.com.cn")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# read date information to know which data should be aggregated or re-aggregated
state_information = load_state_information()

business_zone_bucket_path = "s3a://{}/aggregated/daily".format(args['business_zone_bucket'])

if state_information["first_run"]:
    aggregate_and_write_data_to_s3(business_zone_bucket_path)
else:
    dates_to_process = state_information["dates"]
    if dates_to_process:
        for date in dates_to_process:
            aggregate_and_write_data_to_s3(business_zone_bucket_path, "(reading_type == 'INT' and date_str == '{}')".format(date))

job.commit()
