const AWS = require("aws-sdk");

const documentClient = new AWS.DynamoDB.DocumentClient({
  region: process.env.DEFAULT_AWS,
});

/* insert record in table */
async function handleItems(tableName, record) {

  let params = {
    RequestItems: {
      [`${tableName}`]: record
    }
  }
  console.info("Inserting Records To dynamoDb : \n", params);
  try {
    return await documentClient.batchWrite(params).promise();
  } catch (e) {
    console.error("handleItems Error: ", e);
    return e;
  }
}


async function dbRead(params) {
  try {
    
    let result = await documentClient.scan(params).promise();
    let data = result.Items;
    if (result.LastEvaluatedKey) {
      params.ExclusiveStartKey = result.LastEvaluatedKey;
      data = data.concat(await dbRead(params));
    }
    return data;
  } catch (error) {
    console.info("Error In DbRead()", error);
    return error;
  }
}

/* retrieve all items from table */
async function scanTableData(tableName, status) {
  let params = {
    TableName: tableName,
    FilterExpression: 'api_insert_Status = :status',
    ExpressionAttributeValues: { ':status': status },
  };

  let data = await dbRead(params);
  return data;
}

async function queryRecordsFromDynamoDb(tableName, indexName, billToNumber){
  try {
    let params = { 
      TableName: tableName,
      IndexName: indexName,
      KeyConditionExpression: (indexName == "req_Bill_To_Number__c-index") ? 'req_Bill_To_Number__c = :billToNumber' : 'billToNumber = :billToNumber',
      ExpressionAttributeValues: { ':billToNumber': billToNumber} 
     }
    let result = await documentClient.query(params).promise();
    let data = result.Items;
    return data
  } catch (error) {
    console.info("Error In queryRecordsFromDynamoDb()", error);
    return error;
  }
  
}

module.exports = { handleItems, scanTableData, queryRecordsFromDynamoDb }