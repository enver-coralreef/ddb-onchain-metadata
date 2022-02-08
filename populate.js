const readline = require('readline');
const fs = require('fs');
const AWS = require('aws-sdk');

const inputFile = 'test.data';
const concurrentRequests = 10;
const itemsPerBatch = 25;

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
        batch.push(JSON.parse(line))

        if (batch.length >= itemsPerBatch) {
            promises.push(sendDdbRequest(batch));
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
        await sendDdbRequest(batch);
    }

    console.log('Done!');
}

const sendDdbRequest = async (batch) => {
    const putReqs = batch.map(item => ({
        PutRequest: {
            Item: item
        }
    }));
    const req = {
        RequestItems: {
            'nft-metadata-test': putReqs
        }
    };

    await dynamodb.batchWrite(req).promise();
}

main();