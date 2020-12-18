"use strict"

const AthenaExpress = require("athena-express"),
    aws = require("aws-sdk")

/* AWS Credentials are not required here 
/* because the IAM Role assumed by this Lambda 
/* has the necessary permission to execute Athena queries 
/* and store the result in Amazon S3 bucket */

// outage?start_date_time={}&end_date_time={}
exports.handler = async (event, context, callback) => {

    let dbname = process.env.Db_schema
    let queryParameter = {}

    if (!('queryStringParameters' in event) ||
        !('start_date_time' in event.queryStringParameters) ||
        !('end_date_time' in event.queryStringParameters)
    ) {
        let response = {
            "statusCode": 500,
            "body": "Query parameter couldn't be found in event.",
            "isBase64Encoded": false
        }

        return callback(null, response)
    }

    queryParameter = event.queryStringParameters

    const athenaExpressConfig = {
        aws,
        db: dbname,
        getStats: true
    }
    const athenaExpress = new AthenaExpress(athenaExpressConfig)

    // TODO: Change following to correct error code
    const errorCode = 'INT'

    console.log(queryParameter)
    let startDateTime = queryParameter.start_date_time
    let endDateTime = queryParameter.end_date_time

    const sqlQuery = `SELECT d.*, g.col1 as lat, g.col2 as long FROM daily d, geodata g WHERE d.meter_id = g.col0 AND d.reading_type = '${errorCode}' AND d.reading_date_time BETWEEN TIMESTAMP '${startDateTime}' AND TIMESTAMP '${endDateTime}'`
    console.log(sqlQuery)
    //const sqlQuery = "SELECT * FROM daily WHERE reading_type = '11' AND reading_date_time BETWEEN TIMESTAMP '2010-01-03 09:00:01' AND TIMESTAMP '2010-01-03 10:59:59'"

    try {
        let queryResults = await athenaExpress.query(sqlQuery)

        let response = {
            "statusCode": 200,
            "body": JSON.stringify(queryResults),
            "isBase64Encoded": false
        }

        callback(null, response)
    } catch (error) {
        callback(error, null)
    }
}
