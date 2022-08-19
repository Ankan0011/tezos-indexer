

export function csvFileWriter(data,csvWriter){
    try {
        csvWriter.writeRecords(data);
    }
    catch(error){
        console.log(error);
    }
}