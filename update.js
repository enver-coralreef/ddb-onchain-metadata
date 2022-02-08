const readline = require('readline');
const fs = require('fs');
const AWS = require('aws-sdk');

const inputFile = 'test_2.pkey';
const concurrentRequests = 10;
const itemsPerBatch = 25;
const tableName = 'nft-metadata-test';

AWS.config.loadFromPath('./config.json');

const dynamodb = new AWS.DynamoDB.DocumentClient({apiVersion: '2012-08-10'});

const main = async () => {
    const readStream = fs.createReadStream(inputFile, {encoding: 'utf8'});

    const rl = readline.createInterface({
        input: readStream,
        crlfDelay: Infinity
    });

    let batch = [];
    let promises = [];
    let batchPromisesDone = 0;
    for await (const line of rl) {
        batch.push(line);

        if (batch.length >= itemsPerBatch) {
            promises.push(batchRequest(batch));
            batch = [];
        }

        if (promises.length >= concurrentRequests) {
            await Promise.all(promises);
            batchPromisesDone += 1;
            console.log('Items done: ' + batchPromisesDone * concurrentRequests * itemsPerBatch);
            promises = [];
        }
    }
    if (batch.length > 0) {
        await batchRequest(batch);
    }

    console.log('Done!');
}

const batchRequest = async (mintList) => {
    const params = {
        TransactItems: mintList.map(mint => {
            return {
                Update: updateRequest(mint)
            };
        })
    }    

    await dynamodb.transactWrite(params).promise();
}

const updateRequest = (mint) => {
    const params = {
        TableName: tableName,
        Key: {
            mint
        },
        UpdateExpression: 'SET offChainData = :val1',
        ExpressionAttributeValues: {
            ':val1': {}
        }
    };

    return params;
}

const removeRequest = (mint) => {
    const params = {
        TableName: tableName,
        Key: {
            mint
        },
        UpdateExpression: 'REMOVE offChainData',
        ReturnValues: 'ALL_NEW'
    }

    return params;
}

main();