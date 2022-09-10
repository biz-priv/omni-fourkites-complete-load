const { Client } = require("pg");
const { startXlsxS3Process, fetchAllKeysFromS3, moveS3ObjectToArchive, readS3Object, moveS3ObjectToProcessed } = require('../shared/s3/index');
const { sendResponse } = require('../shared/utils/responses');
const Dynamo = require("../shared/dynamoDb/index");
const { handleFourkitesRequest } = require('../tl-dispatch/handleFourkitesRequests');
const { itemInsertIntoExcel } = require("../shared/excelSheets/index");
const { sendEmail } = require('../shared/sendEmail/index');
// const SSM = require("../shared/ssm/index");
let s3FileName;
let s3BucketName;

async function handleDBOperation(client) {
    try {
        console.info("creating database connectivity");
        let sqlQuery = `Select TOP 15000 shipper_name,reference_nbr,op_carrier_scac,truck_trailer_nbr,truck_trailer_nbr,latitude,longitude,city,state,event_date,pod_date,file_nbr 
        from fourkites_tl`;
        console.info("executing database query");
        let dbResponse = await client.query(sqlQuery);
        let result = dbResponse.rows;
        console.log("Db response length : ", result.length);
        return result;    
    } catch (error) {
        console.error("Error in Fetching database Records : ",error);
        // return sendResponse(400,error);
        return error
    }
}

async function updateRedshiftRecords(client,uploadRedshiftQueries) {
    try {
        for( let index in uploadRedshiftQueries){
            let sqlUpdateQuery = `UPDATE public.fourkites_tl SET message_sent = 'Y' where file_nbr = '${uploadRedshiftQueries[index]['file_nbr']}' and reference_nbr = '${uploadRedshiftQueries[index]['billoflading']}'`;
            console.info("Executing Update Query For file_nbr = " + uploadRedshiftQueries[index]['file_nbr']);
            let dbResponse = await client.query(sqlUpdateQuery);
        }
        return [ true, "success" ]
    } catch (error) {
        console.info('Update In Redshift Error : ',error);
        return [ false, error ]
    }
}

module.exports.handler = async (event) => {
    try {
        const client = new Client({
            database: process.env.DB_DATABASE,
            host: process.env.DB_HOST,
            port: process.env.DB_PORT,
            user: process.env.DB_USER,
            password: process.env.DB_PASSWORD,
        });
        await client.connect();
        let hasMoreData = "";
        s3BucketName = process.env.S3_BUCKET_NAME;
        let s3Path = "liveData"
        let dynamoDbTableName = process.env.FOURKITES_TABLENAME;
        if (event && event.hasOwnProperty("hasMoreData") && event.hasMoreData == "true") {
            hasMoreData = "false";
        } else {
            hasMoreData = "true";
            let result = await handleDBOperation(client);
            if (result.length > 0) {
                await startXlsxS3Process(s3BucketName, result, s3Path);
            }
            else {
                hasMoreData = "false";
                console.info("No Records Found");
                return { hasMoreData };
            }
        }
        console.info("all the files uploaded to s3");
        let s3Keys = await fetchAllKeysFromS3(s3BucketName);
        console.info("S3 Keys : \n", s3Keys);
        if (!s3Keys.length) {
            hasMoreData = false;
            return { hasMoreData };
        }
        s3FileName = s3Keys[0]['Key'];
        let s3Results = await readS3Object(s3BucketName, s3FileName);
        let result = s3Results;
        console.info("result length : ", result.length);
        if (!result.length) {
            hasMoreData = false;
            console.info("No Records Found");
            return { hasMoreData };
        }
        DbDataCount = s3Keys.length;

        let dynamoDbDataArray = [];
        let fileNumberArray = [];
        let fourkitesFailedRecordsArray = [];
        let uploadRedshiftQueries = [];
        let url = process.env.FOURKITES_URL

        for (let keys in result) {
            let fourKitesFailedRecordsObj = {};
            let shipper = result[keys]['shipper_name'] ? result[keys]['shipper_name'] : "";
            let billoflading = result[keys]['reference_nbr'] ? result[keys]['reference_nbr'] : "";
            let operatingCarrierScac = result[keys]['op_carrier_scac'] ? result[keys]['op_carrier_scac'] : "";
            let trucktrailerNumber = result[keys]['truck_trailer_nbr'] ? result[keys]['truck_trailer_nbr'] : "";
            let latitude = result[keys]['latitude'] ? result[keys]['latitude'] : "";
            let longitude = result[keys]['longitude'] ? result[keys]['longitude'] : "";
            let city = result[keys]['city'] ? result[keys]['city'] : "";
            let state = result[keys]['state'] ? result[keys]['state'] : "";
            let locatedAt = result[keys]['event_date'] ? result[keys]['event_date'] : "";
            let deliveredAt = result[keys]['pod_date'] ? result[keys]['pod_date'] : "";
            let fileNumber = result[keys]['file_nbr'] ? result[keys]['file_nbr'] : "";

            let authentication = {
                auth: {
                    username: process.env.FOURKITES_USERNAME,
                    password: process.env.FOURKITES_PASSWORD
                }
            }
            let params = {
                updates: [{
                    shipper: shipper,
                    billOfLading: billoflading,
                    operatingCarrierScac: operatingCarrierScac,
                    truckNumber: trucktrailerNumber,
                    trailerNumber: trucktrailerNumber,
                    latitude: latitude,
                    longitude: longitude,
                    city: city,
                    state: state,
                    locatedAt: locatedAt,
                    deliveredAt: deliveredAt
                }]
            }

            let fourkitesResponse = await handleFourkitesRequest(url, params, authentication);

            if (fourkitesResponse != false) {
                let dynamoDbData = {
                    PutRequest: {
                        Item: {
                            FileNumber: fileNumber + billoflading,
                            BillOfLading: billoflading,
                            Shipper: shipper,
                            OperatingCarrierScac: operatingCarrierScac,
                            TruckNumber: trucktrailerNumber,
                            TrailerNumber: trucktrailerNumber,
                            Latitude: latitude,
                            Longitude: longitude,
                            City: city,
                            State: state,
                            LocatedAt: locatedAt,
                            DeliveredAt: deliveredAt,
                            RecordInsertTime: new Date().toISOString,
                            ApiInsertStatus: true
                        }
                    }
                };
                
                if (!fileNumberArray.includes(fileNumber + billoflading)) {
                    fileNumberArray.push(fileNumber + billoflading);
                    dynamoDbDataArray.push(dynamoDbData);
                };

                if (dynamoDbDataArray.length >= 20) {
                    await Dynamo.handleItems(dynamoDbTableName, dynamoDbDataArray);
                    dynamoDbDataArray = [];
                    fileNumberArray = [];
                }
                uploadRedshiftQueries.push({
                    file_nbr : fileNumber,
                    billoflading : billoflading
                })
                // let updateInRedshift = await updateRedshiftRecords(client, fileNumber, billoflading);
            } else {
                let dynamoDbData = {
                    PutRequest: {
                        Item: {
                            FileNumber: fileNumber + billoflading,
                            BillOfLading: billoflading,
                            Shipper: shipper,
                            OperatingCarrierScac: operatingCarrierScac,
                            TruckNumber: trucktrailerNumber,
                            TrailerNumber: trucktrailerNumber,
                            Latitude: latitude,
                            Longitude: longitude,
                            City: city,
                            State: state,
                            LocatedAt: locatedAt,
                            DeliveredAt: deliveredAt,
                            RecordInsertTime: new Date().toISOString,
                            ApiInsertStatus: false
                        }
                    }
                };
                fourKitesFailedRecordsObj['Status'] = "Failed";
                fourKitesFailedRecordsObj['Url'] = url;
                fourKitesFailedRecordsObj['Params'] = params;
                fourkitesFailedRecordsArray.push(fourKitesFailedRecordsObj);

                if (!fileNumberArray.includes(fileNumber + billoflading)) {
                    fileNumberArray.push(fileNumber + billoflading);
                    dynamoDbDataArray.push(dynamoDbData);
                };

                if (dynamoDbDataArray.length >= 20) {
                    await Dynamo.handleItems(dynamoDbTableName, dynamoDbDataArray);
                    dynamoDbDataArray = [];
                    fileNumberArray = [];
                }
            }
        }
        if(uploadRedshiftQueries.length){
            let updateInRedshift = await updateRedshiftRecords(client, uploadRedshiftQueries);
            console.info("updateInRedshift : ",updateInRedshift);
        }
        if (dynamoDbDataArray.length > 0) {
            await Dynamo.handleItems(dynamoDbTableName, dynamoDbDataArray)
        }
        console.info("fourkitesFailedRecordsArray length : ",fourkitesFailedRecordsArray.length);
        if (fourkitesFailedRecordsArray.length > 0) {
            try {
                const fourkitesFailedRecordsCount = fourkitesFailedRecordsArray.length;
                console.info("fourkitesFailed Error Records Count : " + fourkitesFailedRecordsCount);
                
                /************************ Preparing CSV file For Failed Records ************************/
                await itemInsertIntoExcel(fourkitesFailedRecordsArray);
                /************************ Sending Email ************************/
                let mailSubject = "Fourkites Failed Records";
                let mailBody = "Hello,<br>Total FourKites Error Records Count : <b>" + fourkitesFailedRecordsCount + "</b><br>" + "<b>PFA report for failed records Of Fourkites APIs.</b><Br>Thanks."
                // await sendEmail(mailSubject, mailBody);
            }
            catch (emailExcelError) {
                console.error("emailExcelError :\n", emailExcelError);
            }
        }
        await client.end();
        
        console.info("Moving File to processed_success/");
        await moveS3ObjectToProcessed(s3BucketName, s3FileName, "processed_success/");
        console.info("Moved File to processed_success/");
        // return sendResponse(200);
    } catch (error) {
        console.error("error : ", error);
        console.info("Moving failed File to processed_failed/");
        await moveS3ObjectToProcessed(s3BucketName, s3FileName, "processed_failed/");
        console.info("Moved failed File to processed_failed/");
        // return sendResponse(400, error);
    }
    if (DbDataCount <= 1) {
        hasMoreData = "false";
      } else {
        hasMoreData = "true";
      }
      return { hasMoreData };
}