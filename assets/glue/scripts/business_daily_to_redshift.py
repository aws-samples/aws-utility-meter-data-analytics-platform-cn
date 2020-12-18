import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

create_table_sql = "create table if not exists daily("\
                        "meter_id	        VARCHAR(MAX)    ,   "\
                        "reading_value	    FLOAT8          ,   "\
                        "obis_code	        VARCHAR(32)     , "\
                        "week_of_year	    INT4            , "\
                        "day_of_month	    INT4            , "\
                        "month	            INT4            , "\
                        "year	            INT4            , "\
                        "hour	            INT4            , "\
                        "minute	            INT4            , "\
                        "reading_date_time	timestamp       , "\
                        "reading_type	    VARCHAR(32)     , "\
                        "date_str	        VARCHAR(16)      )"\
                        "DISTKEY(meter_id) SORTKEY(year, meter_id);"

## @params: [TempDir, JOB_NAME, db_name, redshift_connection, temp_workflow_bucket]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME', 'db_name', 'redshift_connection', 'temp_workflow_bucket', 'region'])

sc = SparkContext()
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3."+args['region']+".amazonaws.com.cn")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

allDaily = glueContext.create_dynamic_frame.from_catalog(database = args['db_name'], \
                                                         table_name = "daily", \
                                                         transformation_ctx = "allDaily")

glueContext.write_dynamic_frame.from_jdbc_conf(frame = allDaily, \
                                               catalog_connection = args['redshift_connection'], \
                                               connection_options = {"preactions":create_table_sql, "dbtable": "daily", "database": args['db_name']}, \
                                               redshift_tmp_dir = args["TempDir"], transformation_ctx = "allDaily")

job.commit()