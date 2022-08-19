import fetch from 'node-fetch';
import { createObjectCsvWriter } from 'csv-writer';
import {csvFileWriter} from './resources.js';

//const URL = "http://localhost:5000/v1/accounts/?";
const URL = "https://api.tzkt.io/v1/delegates?";
const records = 5;

const csvWriter = createObjectCsvWriter({
    path: './data/delegates.csv',
    headerIdDelimiter: '.',
    header: [
        {id: 'id', title: 'Id'},
        {id: 'address', title: 'address'},
        {id: 'type', title: 'type'},
        {id: 'active', title: 'active'},
        {id: 'balance', title: 'balance'},
        {id: 'frozenDeposit', title: 'frozen_deposit'},
        {id: 'counter', title: 'counter'},
        {id: 'activationLevel', title: 'activationLevel'},
        {id: 'activeTokensCount', title: 'activeTokensCount'},
        {id: 'stakingBalance', title: 'stakingBalance'},
        {id: 'firstActivityTime', title: 'firstActivityTime'},
        {id: 'lastActivityTime', title: 'lastActivityTime'}
    ]
})

async function getDelegates() {
    const response = await fetch(URL + new URLSearchParams({
        limit: records
    }).toString());
    
    const data = await response.json();
    csvFileWriter(data,csvWriter);
};

getDelegates();