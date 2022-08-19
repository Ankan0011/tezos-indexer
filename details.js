import fetch from 'node-fetch';
import { createObjectCsvWriter } from 'csv-writer';
import {csvFileWriter} from './resources.js';

const URL = "https://api.tzkt.io/v1/accounts?";
const URL2= "https://api.tzkt.io/v1/accounts/{address}/operations";
const records = 2;

const csvWriter = createObjectCsvWriter({
    path: './data/accountOperations.csv',
    headerIdDelimiter: '.',
    header: [
        {id: 'id', title: 'Id'},
        {id: 'level', title: 'level'},
        {id: 'type', title: 'type'},
        {id: 'timestamp', title: 'timestamp'},
        {id: 'timestamp', title: 'timestamp'},
        {id: 'initiator.address', title: 'initiator_address'},
        {id: 'amount', title: 'amount'}
    ]
})

async function getAccountDetails(address) {
    console.log(address);
    const response = await fetch(URL2.replace("{address}", address));
    const data = await response.json();
    console.log(data[0]);
    await csvFileWriter(data,csvWriter);
}

async function getAccounts() {
    const response = await fetch(URL + new URLSearchParams({
        limit: records
    }).toString()
    );
    const data = await response.json();
    //console.log(data[0]);

    // Fetches the head of the object and find the length of the object
    const propOwn = Object.getOwnPropertyNames(data);
    //console.log("This is the length of the json file :"+propOwn.length);

    for (let i = 0; i < propOwn.length-1; i++) {
        getAccountDetails(data[i].address);
      }
};

getAccounts();

