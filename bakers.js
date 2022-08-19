import fetch from 'node-fetch';
import { createObjectCsvWriter } from 'csv-writer';
import {csvFileWriter} from './resources.js';

const URL = "https://api.tzkt.io/v1/delegates?";
const URL2= "https://api.tzkt.io/v1/rewards/bakers/{address}";
const records = 2;

const csvWriter = createObjectCsvWriter({
    path: './data/bakersReward.csv',
    headerIdDelimiter: '.',
    header: [
        {id: 'cycle', title: 'cycle'},
        {id: 'stakingBalance', title: 'stakingBalance'},
        {id: 'activeStake', title: 'activeStake'},
        {id: 'expectedBlocks', title: 'expectedBlocks'},
        {id: 'delegatedBalance', title: 'delegatedBalance'},
        {id: 'numDelegators', title: 'numDelegators'},
        {id: 'futureBlocks', title: 'futureBlocks'},
        {id: 'blocks', title: 'blocks'},
        {id: 'blockRewards', title: 'blockRewards'},
        {id: 'endorsements', title: 'endorsements'},
        {id: 'endorsementRewards', title: 'endorsementRewards'},
        {id: 'futureBlocks', title: 'futureBlocks'}
    ]
})

async function getBakersDetails(address) {
    //console.log(address);
    const response = await fetch(URL2.replace("{address}", address));
    const data = await response.json();
    //console.log(data[0]);
    await csvFileWriter(data,csvWriter);
}

async function getDelegates() {
    const response = await fetch(URL + new URLSearchParams({
        limit: records
    }).toString());
    const data = await response.json();

    // Fetches the head of the object and find the length of the object
    const propOwn = Object.getOwnPropertyNames(data);
    //console.log("This is the length of the json file :"+propOwn.length);

    for (let i = 0; i < propOwn.length-1; i++) {
        getBakersDetails(data[i].address);
      }
};

getDelegates();