import fetch from "node-fetch";
import { createObjectCsvWriter } from 'csv-writer';

var fs = import('fs')

const URL = "https://randomuser.me/api/";

//const createCsvWriter = import('csv-writer').createCsvWriter;

const csvWriter = createObjectCsvWriter({
    path: './forecast.csv',
    headerIdDelimiter: '.',
    header: [
        {id: 'gender', title: 'Gender'},
        {id: 'email', title: 'Email'},
        {id: 'name.first', title: 'First'}
    ]
    //header: ['title', 'first', 'last'].map((item)=> ({ id: item, title: item}))
})

async function getRandomUserData() {
    const response = await fetch(URL);
    const data = await response.json();
    console.log(data.results[0]);

    // Fetches the head of the object and find the length of the object
    const propOwn = Object.getOwnPropertyNames(data.results[0]);
    console.log("This is the length of the json file :"+propOwn.length);

    try {
        await csvWriter.writeRecords(data.results);
    }
    catch(error){
        console.log(error);
    }
};

getRandomUserData();