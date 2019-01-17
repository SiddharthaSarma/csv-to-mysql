const fs = require('fs');
const csv = require('csv-parser');
const inputFilePath = './prices.csv';
var mysql = require('mysql');
var connection = mysql.createConnection({
  user: 'newuser',
  password: 'password',
  database: 'stocks_db'
});
connection.connect();
var records = [];
var i = 0;
fs.createReadStream(inputFilePath)
  .pipe(csv())
  .on('data', function(data) {
    try {
      var date = data.date.split(' ')[0];
      // console.log(date);
      // date = date.split('-');
      // var formattedDate = `${date[2]}-${date[1]}-${date[0]}`;
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
  var sql =
    'INSERT INTO `stocks_demo2_tb` (`date`,`symbol`, `open`, `close`, `low`, `high`, `volume`) VALUES ?';

  records.shift();
  var recordBreak = 100000;
  for (var i = 0; i < Math.ceil(records.length / recordBreak); i++) {
    var recordsList = records.slice(i * recordBreak, (i + 1) * recordBreak);
    await connection.query(sql, [recordsList], function(err) {
      if (err) throw err;
      console.log('hey');
    });
  }
  console.log('done');
};
