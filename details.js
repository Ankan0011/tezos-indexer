import fetch from 'node-fetch';
import { createObjectCsvWriter } from 'csv-writer';
import {csvFileWriter} from './resources.js';

const URL = "https://api.tzkt.io/v1/accounts?";
const URL2= "https://api.tzkt.io/v1/accounts/{address}/operations";
const records = 2000;

const csvWriter = createObjectCsvWriter({
    path: './data/test/accountOperations.csv',
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

function handleErrors(response) {
    if (!response.ok) {
        throw Error(response.statusText);
    }
    return response;
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function getAccountDetails(address) {
    console.log(address);
    const response =  fetch(URL2.replace("{address}", address))
    //.then(handleErrors)
    .then(res => {
        if (!res.ok) {
            throw new Error(); // Will take you to the `catch` below
        }
        return res.json();
    })
    .then( data => { 
        //console.log(data[0]); 
        csvFileWriter(data,csvWriter);
    }).catch( error => { 
        sleep(2000);
        console.log(error);}
    );
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

