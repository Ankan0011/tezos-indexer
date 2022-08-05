var fs = import('fs')

const createCsvWriter = import('csv-writer').createObjectCsvWriter;

const csvWriter = createCsvWriter({
    path: './forecast.csv',
    header: ['time', 'temp_c', 'temp_f'].map((item)=> ({ id: item, title: item}))
})


async function main(){
    const file_data = await fs.readFile('file.json');
    const parse_data = JSON.parse(file_data);

    try{
         await csvWriter.writeRecords(parse_data.forecastday[0].hour);
    }
    catch(error){
        console.log(error);
    }

}

main()