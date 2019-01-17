const fs = require('fs');
const csv = require('csv-parser');
const inputFilePath = './prices.csv';
const mysql = require('mysql');

const connection = mysql.createConnection({
  user: 'newuser',
  password: 'password',
  database: 'stocks_db'
});
connection.connect();
const records = [];
let i = 0;
fs.createReadStream(inputFilePath)
  .pipe(csv())
  .on('data', function(data) {
    try {
      let date = data.date.split(' ')[0];
      // console.log(date);
      date = date.split('-');
      date = `${date[2]}-${date[1]}-${date[0]}`;
      records.push([
        date,
        data.symbol,
        parseFloat(data.open),
        parseFloat(data.close),
        parseFloat(data.low),
        parseFloat(data.high),
        parseFloat(data.volume)
      ]);
      i++;
      console.log(i);
    } catch (err) {
      //error handler
    }
  })
  .on('end', function() {
    dumpInToDB();
  });

const dumpInToDB = async () => {
  const promises = [];
  const sql =
    'INSERT INTO `stocks_demo2_tb` (`date`,`symbol`, `open`, `close`, `low`, `high`, `volume`) VALUES ?';

  records.shift();
  const recordBreak = 10;
  const maxIterations = Math.ceil(records.length / recordBreak);
  for (let i = 0; i < maxIterations; i++) {
    const recordsList = records.slice(i * recordBreak, (i + 1) * recordBreak);
    try {
      promises.push(await connection.query(sql, [recordsList]));
      // console.log(result);
    } catch (err) {
      console.err(err);
    }
  }
  await Promise.all(promises);
  await connection.end();
  console.log('done');
};
