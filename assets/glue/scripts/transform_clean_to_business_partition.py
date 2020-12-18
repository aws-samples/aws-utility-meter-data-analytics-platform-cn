import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'db_name', 'table_name', 'business_zone_bucket', 'region'])

sc = SparkContext()
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3."+args['region']+".amazonaws.com.cn")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

tableName = args['table_name'].replace("-", "_")
cleanedMeterDataSource = glueContext.create_dynamic_frame.from_catalog(database = args['db_name'], table_name = tableName, transformation_ctx = "cleanedMeterDataSource")

business_zone_bucket_path_daily = "s3a://{}/daily".format(args['business_zone_bucket'])

businessZone = glueContext.write_dynamic_frame.from_options(frame = cleanedMeterDataSource, \
    connection_type = "s3", \
    connection_options = {"path": business_zone_bucket_path_daily, "partitionKeys": ["reading_type", "date_str"]},\
    format = "parquet", \
    transformation_ctx = "businessZone")

job.commit()