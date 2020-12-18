import json, os, datetime
import psycopg2
import boto3

REGION = os.environ['AWS_REGION']


class JSONDateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        else:
            return json.JSONEncoder.default(self, obj)

def get_sql_statement(requested_aggregation):
    if requested_aggregation == "weekly":
        return """
                select meter_id, year, week_of_year, sum(reading_value) 
                    from daily 
                    where meter_id=%s AND year=%s 
                    group by week_of_year, year, meter_id 
                    order by week_of_year
                """
    elif requested_aggregation == "monthly":
        return """select meter_id, year, month, sum(reading_value) 
                        from daily 
                        where meter_id=%s AND year=%s 
                        group by year, month, meter_id 
                        order by month
            """
    else:
        # make daily the default, or raise an exception
        return "select meter_id, date_str, sum(reading_value) from daily where meter_id=%s AND year=%s group by date_str, meter_id"


def load_redshift_cred():
    secret_name = os.environ["SECRET_NAME"]

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=REGION
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )

    except ClientError as e:
        raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])

    return json.loads(secret)


def lambda_handler(event, context):
    dbname = os.environ['Db_schema']
    parameter = event["pathParameters"]

    if ("meter_id" not in parameter) or ("year" not in parameter) or ("requested_aggregation" not in parameter):
        return {
            'statusCode': 500,
            'body': "error: meter_id, year and aggregation type needs to be provided."
        }

    meter_id = parameter["meter_id"]
    year = parameter["year"]
    requested_aggregation = parameter["requested_aggregation"]

    redshift_cred = load_redshift_cred()

    connection = psycopg2.connect(user = redshift_cred["username"],
                                  password = redshift_cred["password"],
                                  host = redshift_cred["host"],
                                  port = redshift_cred["port"],
                                  database = dbname)

    cursor = connection.cursor()

    sql = get_sql_statement(requested_aggregation)

    cursor.execute(sql, (meter_id, year,))
    daily_reads = cursor.fetchall()

    return {
        'statusCode': 200,
        'body': json.dumps(daily_reads, cls=JSONDateTimeEncoder)
    }
