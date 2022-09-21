import psycopg2
import logging
import json
import datetime
from datetime import datetime,timezone
import boto3
import requests
from requests.auth import HTTPBasicAuth
import xlsxwriter
logger = logging.getLogger()
logger.setLevel(logging.INFO)
client = boto3.client('dynamodb')
from boto3.dynamodb.conditions import Key, Attr

from src.common import modify_date
from src.common import execute_db_query
from src.common import s3UploadObject

def handler(event, context):
    try :
        logger.info("Event: {}".format(json.dumps(event)))
        query = 'Select shipper_name,reference_nbr,op_carrier_scac,truck_trailer_nbr,truck_trailer_nbr,latitude,longitude,city,state,event_date,pod_date,file_nbr from fourkites_tl where message_sent = '''
        queryData = execute_db_query(query)
        logger.info("queryData Response from redshift : ", queryData)
        s3BucketName = ''
        # startXlsxS3Process(s3BucketName, queryData, 'liveData')
        for results in queryData:
            temp = recordsConv(results,con)
            records_list.append(temp)
        
        shipment_records = {'updates':records_list}
        payload = json.dumps(shipment_records)
        logger.info("Payload loaded into Fourkites API  :{}".format(payload))
        headers = {'content-type': 'application/json'}
        # r = requests.post(url, headers=headers,data=payload,auth=HTTPBasicAuth(os.environ['fourkites_username'],os.environ['fourkites_password']))
        # logger.info("Response from fourkites API :{}".format(r))
    except Exception as e:
            logging.exception("ApiPostError: {}".format(e))
            raise ApiPostError(json.dumps({"httpStatus": 400, "message": "Api post error."}))    

def startXlsxS3Process(s3BucketName, requestData, path):
    for reqData in requestData:
        dataToUpload =  prepareSpreadsheet(reqData)
        uploadFileToS3(s3BucketName, dataToUpload, path)
        
def prepareSpreadsheet(reqData):
    try:    
        workbook = xlsxwriter.Workbook('sample.xlsx')
        worksheet = workbook.add_worksheet()
        col = 0

        for row, data in enumerate(reqData):
            worksheet.write_row(row, col, data)

        workbook.close()
        return workbook
    except Exception as e:
        logging.exception("WorkbookError: {}".format(e))
        raise WorkBookCreationError(json.dumps({"httpStatus": 400, "message": "Workbook creation error."}))
    # wb = XLSX.utils.book_new()
    # sheetData = XLSX.utils.json_to_sheet(reqData)
    # XLSX.utils.book_append_sheet(wb, sheetData, 'Sheet 1')
    # sheetDataBuffer = await XLSX.write(wb, { bookType: 'csv', type: 'buffer', bookSST: false })
    # sheetDataBuffer

def uploadFileToS3(s3BucketName, sheetDataBuffer, path):
    try:
        s3UploadObject(sheetDataBuffer,path,s3BucketName,"key")
    except Exception as e:
        logging.exception("RecordConversionError: {}".format(e))
        raise UploadFileS3Error(json.dumps({"httpStatus": 400, "message": "Upload File to S3 error."}))
    

def recordsConv(y,con):
    try:
        record = {}
        record["shipper"] = y[0]
        record["billOfLading"] = y[1]
        record["operatingCarrierScac"] = y[2]
        record["truckNumber"] = y[3]
        record["trailerNumber"] = y[4]
        record["latitude"] = y[5]
        record["longitude"] = y[6]
        record["city"] = y[7]
        record["state"] = y[8]
        record["locatedAt"] = modify_date(y[9])
        record["deliveredAt"] = modify_date(y[10])
        shipper = y[0]
        billoflading = y[1]
        operatingCarrierScac = y[2]
        truckNumber = y[3]
        trailerNumber = y[4]
        latitude = y[5]
        longitude = y[6]
        city = y[7]
        state = y[8]
        locatedAt = record["locatedAt"]
        deliveredAt = record["deliveredAt"]
        file_nbr = y[11]

        updateDynamoDB(shipper,billoflading,operatingCarrierScac,truckNumber,trailerNumber,latitude,longitude,city,state,locatedAt,deliveredAt,file_nbr)
        cur = con.cursor()
        cur.execute(f"UPDATE public.fourkites_tl SET message_sent = 'Y' where file_nbr = '{file_nbr}' and reference_nbr = '{billoflading}'")
        con.commit()
        return record
        cur.close()
        con.close()
    except Exception as e:
        logging.exception("RecordConversionError: {}".format(e))
        raise RecordConversionError(json.dumps({"httpStatus": 400, "message": "Record conversion error."}))


def updateDynamoDB(shipper,billoflading,operatingCarrierScac,truckNumber,trailerNumber,latitude,longitude,city,state,locatedAt,deliveredAt,file_nbr):
    try:
        x = datetime.now()
        x = dateconv(x)
        response = client.put_item(
            TableName = os.environ['fourkites_tablename'],
            Item={
                'FileNumber': {
    		    'S': file_nbr+billoflading
    		    },
    		    'BillOfLading': {
    		    'S': billoflading
    		    },
    		    'operatingCarrierScac':{
    		    'S': operatingCarrierScac
    		    },
    		    'truckNumber':{
    		    'S': truckNumber
    		    },
    		    'trailerNumber':{
    		    'S': trailerNumber
    		    },
    		    'latitude':{
    		    'S': latitude
    		    },
    		    'longitude':{
    		    'S': longitude
    		    },
    		    'city':{
    		    'S': city
    		    },
    		    'state':{
    		    'S': state
    		    },
    		    'locatedAt':{
    		    'S': locatedAt
    		    },
    		    'deliveredAt':{
    		    'S': deliveredAt
    		    },
    		    'RecordInsertTime':{
    		    'S': x
    		    }
            }
            )
        return response
    except Exception as e:
        logging.exception("UpdateDynamodbError: {}".format(e))
        raise UpdateDynamodbError(json.dumps({"httpStatus": 400, "message": "DynamoDB Update Error"}))

class RecordConversionError(Exception): pass
class ApiPostError(Exception): pass
class UpdateDynamodbError(Exception): pass
class WorkBookCreationError(Exception): pass
class UploadFileS3Error(Exception): pass