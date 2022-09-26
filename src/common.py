import os
import json
import boto3
import logging
import psycopg2
from datetime import datetime,timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def modify_date(x):
    try:
        if x == None:
            return 'null'
        else:
            return x.isoformat()
    except Exception as e:
        logging.exception("DateConversionError: {}".format(e))
        raise DateConversionError(json.dumps({"httpStatus": 501, "message": InternalErrorMessage}))

def execute_db_query(query):
    try:
        logger.info("os.environ['db_name'] : {} ".format(json.dumps(os.environ['db_name'])))
        logger.info("os.environ['db_host'] : {}".format(json.dumps(os.environ['db_host'])))
        logger.info("os.environ['db_port'] : {}".format(json.dumps(os.environ['db_port'])))
        logger.info("os.environ['db_username'] : {}".format(json.dumps(os.environ['db_username'])))
        logger.info("os.environ['db_password'] : {}".format(json.dumps(os.environ['db_password'])))
        con=psycopg2.connect(dbname = os.environ['db_name'], host= os.environ['db_host'],
                            port= os.environ['db_port'], user = os.environ['db_username'], password = os.environ['db_password'])
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        #sql file which contains all the main tables create query
        sql_file = open('script.sql','r')
        
        result = cur.execute(sql_file.read())
        con.commit()
        records_list = []
        # for results in cur.fetchall():
        #     temp = recordsConv(results,con)
        #     records_list.append(temp)
        results = cur.fetchall()    
        cur.close()
        con.close()
        logger.info("results  :{}".format(results))
        # cur.execute(query)
        # con.commit()
        # results = cur.fetchall()
        # cur.close()
        # con.close()
        return results
    except Exception as e:
        logging.exception("DatabaseError: {}".format(e))
        raise DatabaseError(json.dumps({"httpStatus": 400, "message": "Database error."}))

def execute_db_query_from_file():
    try:
        logger.info("os.environ['db_name'] : {} ".format(json.dumps(os.environ['db_name'])))
        logger.info("os.environ['db_host'] : {}".format(json.dumps(os.environ['db_host'])))
        logger.info("os.environ['db_port'] : {}".format(json.dumps(os.environ['db_port'])))
        logger.info("os.environ['db_username'] : {}".format(json.dumps(os.environ['db_username'])))
        logger.info("os.environ['db_password'] : {}".format(json.dumps(os.environ['db_password'])))
        con=psycopg2.connect(dbname = os.environ['db_name'], host= os.environ['db_host'],
                            port= os.environ['db_port'], user = os.environ['db_username'], password = os.environ['db_password'])
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        #sql file which contains all the main tables create query
        sql_file = open('script.sql','r')

        result = cur.execute(sql_file.read())
        con.commit()
        results = cur.fetchall()    
        cur.close()
        con.close()
        logger.info("results  :{}".format(results))
        return results
    except Exception as e:
        logging.exception("DatabaseError: {}".format(e))
        raise DatabaseError(json.dumps({"httpStatus": 400, "message": "Database error."}))

def validateDynamoDB(filenumber):
    try:
        response = client.query(
            TableName=os.environ['fourkites_tablename'],
            IndexName=os.environ['fourkites_tableindex'],
            KeyConditionExpression='FileNumber = :FileNumber',
            ExpressionAttributeValues={
                ':FileNumber': {'S': filenumber}
            }
        )
        print(response)
        if not response['Items']:
            return 'success'
        else:
            orderno = response['Items'][0]['FileNumber']['S']
            return orderno
    except Exception as e:
        logging.exception("ValidateDynamodbError: {}".format(e))
        raise ValidateDynamodbError(json.dumps({"httpStatus": 501, "message": "Validate Dynamo error."}))
    
def s3UploadObject(queryData,filename,bucket,key):
    try:
        s3 = boto3.resource('s3')
    except Exception as e:
        logging.exception("S3InitializationError: {}".format(e))
        raise InitializationError(json.dumps({"httpStatus": 400, "message": "s3 initialization error."})) 
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(str(queryData))
        s3.meta.client.upload_file(filename, bucket, key)
    except Exception as e:
        logging.exception("UpdateFileError: {}".format(e))
        raise UpdateFileError(json.dumps({"httpStatus": 400, "message": "Update file error."}))    
        
class DatabaseError(Exception): pass
class ValidateDynamodbError(Exception): pass
class DateConversionError(Exception): pass
class InitializationError(Exception): pass
class UpdateFileError(Exception): pass
