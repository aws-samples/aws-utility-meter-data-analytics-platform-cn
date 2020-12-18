import sys
import boto3
from datetime import datetime
from botocore.errorfactory import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


def check_if_file_exist(bucket, key):
    s3client = boto3.client('s3', region_name=args['region'])
    fileExists = True
    try:
        s3client.head_object(Bucket=bucket, Key=key)
    except ClientError:
        fileExists = False
        pass

    return fileExists


def move_temp_file(bucket, key):
    dt = datetime.now()
    dt.microsecond

    s3 = boto3.resource('s3', region_name=args['region'])
    newFileName = str(dt.microsecond) + '_' + key
    s3.Object(args['temp_workflow_bucket'], newFileName).copy_from(CopySource="{}/{}".format(bucket, key))
    s3.Object(args['temp_workflow_bucket'], key).delete()


def cleanup_temp_folder(bucket, key):
    if check_if_file_exist(bucket, key):
        move_temp_file(bucket, key)


def parseDateString(timestampStr):
    return datetime.strptime(timestampStr, "%Y%m%d%H24%M%S")


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'db_name', 'table_name', 'clean_data_bucket', 'temp_workflow_bucket'])

sc = SparkContext()
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3."+args['region']+".amazonaws.com.cn")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

cleanup_temp_folder(args['temp_workflow_bucket'], 'glue_workflow_distinct_dates')

tableName = args['table_name'].replace("-", "_")
datasource = glueContext.create_dynamic_frame.from_catalog(database = args['db_name'], table_name = tableName, transformation_ctx = "datasource")
applymapping1 = ApplyMapping.apply(frame = datasource, mappings = [\
    ("col0", "long", "meter_id", "string"), \
    ("col1", "string", "obis_code", "string"), \
    ("col2", "long", "reading_time", "string"), \
    ("col3", "long", "reading_value", "double"), \
    ("col4", "string", "reading_type", "string") \
    ], transformation_ctx = "applymapping1")
    
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")

dropnullfields = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields")

mappedReadings = DynamicFrame.toDF(dropnullfields)

# reading_time could not be passed, so splitting date and time fields manually
mappedReadings = mappedReadings.withColumn("date_str", col("reading_time").substr(1,8))

timeStr = regexp_replace(col("reading_time").substr(9,16), "24", "")
time = to_timestamp(timeStr, "HHmmss")
date = to_date(col("date_str"), "yyyyMMdd")

udfParseDateString = udf(parseDateString, TimestampType())

# add separate date and time fields
mappedReadings = mappedReadings.withColumn("week_of_year", weekofyear(date)) \
          .withColumn("day_of_month", dayofmonth(date)) \
          .withColumn("month", month(date)) \
          .withColumn("year", year(date)) \
          .withColumn("hour", hour(time)) \
          .withColumn("minute", minute(time)) \
          .withColumn("reading_date_time", udfParseDateString("reading_time")) \
          .drop("reading_time")

# get the distinct date value and store them in a temp S3 bucket to now which aggregation data need to be
# calculated later on
distinctDates = mappedReadings.select('date_str').distinct().collect()
distinctDatesStrList = ','.join(value['date_str'] for value in distinctDates)

s3 = boto3.resource('s3', region_name=boto3.session.Session().region_name)
s3.Object(args['temp_workflow_bucket'], 'glue_workflow_distinct_dates').put(Body=distinctDatesStrList)

# write data to S3
filteredMeterReads = DynamicFrame.fromDF(mappedReadings, glueContext, "filteredMeterReads")

s3_clean_path = "s3://" + args['clean_data_bucket']

glueContext.write_dynamic_frame.from_options(
    frame = filteredMeterReads,
    connection_type = "s3",    
    connection_options = {"path": s3_clean_path},
    format = "parquet",
    transformation_ctx = "s3CleanDatasink")

job.commit()
