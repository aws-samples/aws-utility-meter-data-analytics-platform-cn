import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME', 'db_name', 'redshift_connection', 'cis_bucket', 'geo_bucket', 'region'])

sc = SparkContext()
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3."+args['region']+".amazonaws.com.cn")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

"""
    Copy CIS Demo Data to Redshift
"""
cis_datasource = glueContext.create_dynamic_frame_from_options("s3", \
                                                               {"paths": ["s3://{}".format(args['cis_bucket'])],"recurse": True}, \
                                                               format="csv", \
                                                               format_options={ \
                                                                   "withHeader": False, \
                                                                   "separator": ";" \
                                                                   })

cis_mapping = ApplyMapping.apply(frame = cis_datasource, mappings = [("col0", "string", "customer_id", "string"), \
                                                                     ("col1", "string", "name", "string"), \
                                                                     ("col2", "long", "zip", "long"), \
                                                                     ("col3", "string", "city", "string"), \
                                                                     ("col4", "string", "state", "string"), \
                                                                     ("col5", "string", "street", "string"), \
                                                                     ("col6", "string", "phone", "string"), \
                                                                     ("col7", "string", "meter_id", "string"), \
                                                                     ("col8", "double", "lat", "double"), \
                                                                     ("col9", "double", "long", "double")\
                                                                     ], transformation_ctx ="cis_mapping")

glueContext.write_dynamic_frame.from_jdbc_conf(frame = cis_mapping, \
                                               catalog_connection = args['redshift_connection'], \
                                               connection_options = {"dbtable": "cis_data", "database": args['db_name']}, \
                                               redshift_tmp_dir = args["TempDir"], \
                                               transformation_ctx = "cisData")

"""
    Copy Geo Demo Data to Redshift
"""
geo_datasource = glueContext.create_dynamic_frame_from_options("s3", \
                                                               {"paths": ["s3://{}".format(args['geo_bucket'])],"recurse": True}, \
                                                               format="csv", \
                                                               format_options={ \
                                                                   "withHeader": False, \
                                                                   "separator": "," \
                                                                   })

geo_mapping = ApplyMapping.apply(frame = geo_datasource, mappings = [("col0", "string", "meter_id", "string"),\
                                                                     ("col1", "string", "lat", "double"), \
                                                                     ("col2", "string", "lng", "double"), \
                                                                     ], transformation_ctx ="geo_mapping")

glueContext.write_dynamic_frame.from_jdbc_conf(frame = geo_mapping, \
                                               catalog_connection = args['redshift_connection'], \
                                               connection_options = {"dbtable": "geo_data", "database": args['db_name']}, \
                                               redshift_tmp_dir = args["TempDir"], \
                                               transformation_ctx = "cisData")

job.commit()