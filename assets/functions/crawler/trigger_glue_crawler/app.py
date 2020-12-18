import boto3


def lambda_handler(event, context):
    crawler_name = event["crawler_name"]

    client = boto3.client('glue')

    crawler_info = client.get_crawler(Name=crawler_name)
    current_state = crawler_info['Crawler']['State']

    if current_state == 'READY':
        client.start_crawler(Name=crawler_name)
        print("Crawler [{}] started".format(crawler_name))
    else:
        print("Crawler [{}] currently in state [{}], could not trigger a new run.".format(crawler_name, current_state))

    return {**event}
