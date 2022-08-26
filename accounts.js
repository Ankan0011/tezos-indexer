import fetch from 'node-fetch';
import { createObjectCsvWriter } from 'csv-writer';
import {csvFileWriter} from './resources.js';

//const URL = "http://localhost:5000/v1/accounts/?";
const URL = "https://api.tzkt.io/v1/accounts?";
const records = 10;

const csvWriter = createObjectCsvWriter({
    path: './data/accounts.csv',
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
        limit: records
    }).toString());
    
    const data = await response.json();
    //console.log(data[0]);
    csvFileWriter(data,csvWriter);
};

getRandomUserData();