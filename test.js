import fetch from 'node-fetch';
import { createObjectCsvWriter } from 'csv-writer';

const URL = "http://localhost:5000/v1/accounts/?";
//const URL = "https://api.tzkt.io/v1/accounts?";

const csvWriter = createObjectCsvWriter({
    path: './accounts.csv',
    headerIdDelimiter: '.',
    header: [
        {id: 'id', title: 'Id'},
        {id: 'address', title: 'address'},
        {id: 'type', title: 'type'},
        {id: 'alias', title: 'alias'},
        {id: 'revealed', title: 'revealed'},
        {id: 'balance', title: 'balance'},
        {id: 'counter', title: 'counter'},
        {id: 'numContracts', title: 'numContracts'},
        {id: 'activeTokensCount', title: 'activeTokensCount'},
        {id: 'numTransactions', title: 'numTransactions'},
        {id: 'firstActivityTime', title: 'firstActivityTime'},
        {id: 'lastActivityTime', title: 'lastActivityTime'}
    ]
})

async function getRandomUserData() {
    const response = await fetch(URL + new URLSearchParams({
        limit: '50'
    }).toString()
    );
    const data = await response.json();
    console.log(data[0]);

    // Fetches the head of the object and find the length of the object
    //const propOwn = Object.getOwnPropertyNames(data.results[0]);
    //console.log("This is the length of the json file :"+propOwn.length);

    try {
        await csvWriter.writeRecords(data);
    }
    catch(error){
        console.log(error);
    }
};

getRandomUserData();