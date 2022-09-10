const AWS = require("aws-sdk");
const s3 = new AWS.S3({ apiVersion: '2006-03-01' });
let XLSX = require('xlsx');
const { csvToJSON } = require("../utils/utils");

async function startXlsxS3Process(s3BucketName, requestData, path) {
    let chunkSize = 150;
    for (let i = 0, len = requestData.length; i < len; i += chunkSize) {
        let dataToUpload = requestData.slice(i, i + chunkSize);
        const sheetDataBuffer = await prepareSpreadsheet(dataToUpload);
        await uploadFileToS3(s3BucketName, sheetDataBuffer, path);
    }
}

async function uploadFileToS3(s3BucketName, sheetDataBuffer, path) {
    try {
        let date = new Date()
        date = date.toISOString()
        date = date.replace(/:/g, '-')
        const params = {
            Bucket: s3BucketName,
            Key: `${path}/${date}.csv`,
            Body: sheetDataBuffer,
            ContentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            ContentEncoding: 'base64',
            ACL: 'private'
        };

        return new Promise((resolve, reject) => {
            s3.upload(params, function (err, data) {
                if (err) {
                    return reject(err);
                }
                return resolve(data.Location);
            });
        });
    } catch (error) {
        return error
    }
}

async function prepareSpreadsheet(requestData) {
    const wb = XLSX.utils.book_new();
    const sheetData = XLSX.utils.json_to_sheet(requestData);
    XLSX.utils.book_append_sheet(wb, sheetData, 'Sheet 1');
    const sheetDataBuffer = await XLSX.write(wb, { bookType: 'csv', type: 'buffer', bookSST: false });
    return sheetDataBuffer;
}

async function fetchAllKeysFromS3(s3BucketName, token) {
    let allKeys = [];
    let opts = { Bucket: s3BucketName, Prefix: 'liveData/' };
    if (token) opts.ContinuationToken = token;
    return new Promise((resolve, reject) => {
        s3.listObjectsV2(opts, function (err, data) {
            allKeys = allKeys.concat(data.Contents);
            if (data.IsTruncated == "true")
                fetchAllKeysFromS3(s3BucketName, data.NextContinuationToken);
            else {
                resolve(allKeys);
            }
        });
    });
}

async function moveS3ObjectToArchive(s3BucketName, fileName) {
        try {
            let params = {
                Bucket: s3BucketName,
                CopySource: s3BucketName + '/' + fileName,
                Key: fileName.replace('liveData/', 'archive/')
            };
            let copyObject = await s3.copyObject(params).promise();
            console.info('Copied :',params.Key)
            let deleteParams = {
                Bucket: s3BucketName,
                Key: fileName,
            }
            let deleteObject = await s3.deleteObject(deleteParams).promise();
            console.info("file deleted : ",JSON.stringify(fileName));
            return "completed";
        } catch (error) {
            console.error("S3 Copy and delete error : ",JSON.stringify(error));
            return error;
        }
}

async function readS3Object(s3BucketName, fileName) {
    try {
        let getParams = {
            Bucket: s3BucketName,
            Key: fileName
        }
        const stream = s3.getObject(getParams).createReadStream().on('error', error => {
            return error
        });
        const data = await csvToJSON(stream);
        return data;
    } catch (error) {
        console.error(error);
        return error.message;
    }
}

async function moveS3ObjectToProcessed(s3BucketName, fileName, folderName) {
    return new Promise(async(resolve, reject) => {
        try {
            let params = {
                Bucket: s3BucketName,
                CopySource: s3BucketName + '/' + fileName,
                Key: fileName.replace('liveData/', folderName)
            };
            console.info("params: ", params);
            let copyData = await s3.copyObject(params).promise();
            console.info("copyData : ",copyData);
            let deleteData = await s3.deleteObject({ Bucket: s3BucketName,Key: fileName}).promise();
            console.info("deleteData : ",deleteData);
            resolve("completed");
        } catch (error) {
            console.error("Error in S3 move to Archive : ",error);
            reject(error)
        }
    });
}

module.exports = { startXlsxS3Process, fetchAllKeysFromS3, moveS3ObjectToArchive, readS3Object, moveS3ObjectToProcessed }