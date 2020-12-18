import boto3


def lambda_handler(event, context):
    crawler_name = event["crawler_name"]

    client = boto3.client('glue')

    crawler_info = client.get_crawler(Name=crawler_name)
    crawler_state = crawler_info['Crawler']['State']

    return {**event, 'crawler_state': crawler_state}
