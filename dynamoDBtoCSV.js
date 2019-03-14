var program = require("commander");
var AWS = require("aws-sdk");
var unmarshal = require("dynamodb-marshaler").unmarshal;
var Papa = require("papaparse");
var fs = require("fs");
var headers = [];
var nl = '\n';

program
  .version("0.0.1")
  .option("-t, --table [tablename]", "Add the table you want to output to csv")
  .option("-d, --describe")
  .option("-r, --region [regionname]")
  .option(
    "-e, --endpoint [url]",
    "Endpoint URL, can be used to dump from local DynamoDB"
  )
  .option("-p, --profile [profile]", "Use profile from your credentials file")
  .option("-f, --file [file]", "Name of the file to be created")
  .option(
    "-ec --envcreds",
    "Load AWS Credentials using AWS Credential Provider Chain"
  )
  .parse(process.argv);

if (!program.table) {
  console.log("You must specify a table");
  program.outputHelp();
  process.exit(1);
}

if (program.region && AWS.config.credentials) {
  AWS.config.update({ region: program.region });
} else {
  AWS.config.loadFromPath(__dirname + "/config.json");
}

if (program.endpoint) {
  AWS.config.update({ endpoint: program.endpoint });
}

if (program.profile) {
  var newCreds = AWS.config.credentials;
  newCreds.profile = program.profile;
  AWS.config.update({ credentials: newCreds });
}

if (program.envcreds) {
  var newCreds = AWS.config.credentials;
  newCreds.profile = program.profile;
  AWS.config.update({
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    },
    region: process.env.AWS_DEFAULT_REGION
  });
}

var dynamoDB = new AWS.DynamoDB();

var query = {
  TableName: program.table,
  Limit: 1000
};

const outputStream = fs.createWriteStream(`./${program.file}`, {
  encoding: 'utf8',
  autoClose: false,
  flags: 'w',
});

var writeChunkToFile = function (chunk) {
  return new Promise((resolve, reject) => {
    outputStream.write(chunk + nl, function (err) {
      if (err) return reject(err);
      return resolve(chunk);
    });
  });
}

var describeTable = function(query) {
  dynamoDB.describeTable(
    {
      TableName: program.table
    },
    function(err, data) {
      if (!err) {
        console.dir(data.Table);
      } else console.dir(err);
    }
  );
};

var scanDynamoDB = function(query, scanIndex = 0) {
  console.log('Batch %s, Records %s', scanIndex + 1, (scanIndex + 1) * 1000);
  return dynamoDB.scan(query).promise()
    .then(data => {
      // unMarshalIntoArray(data.Items); // Print out the subset of results.
      if (data.LastEvaluatedKey) {
        // Result is incomplete; there is more to come.
        query.ExclusiveStartKey = data.LastEvaluatedKey;
        return unMarshalIntoArray(data.Items, scanIndex).then(() => {
          // console.log('Next...');
          return scanDynamoDB(query, scanIndex + 1);
        });
      }
      return unMarshalIntoArray(data.Items, scanIndex).then(() => {
        // console.log('Done');
        outputStream.close();
      });
    })
};

function unMarshalIntoArray(items, scanIndex) {
  if (items.length === 0) return;

  const rows = items.map(function(row) {
    let newRow = {};
    // console.log( 'Row: ' + JSON.stringify( row ));
    Object.keys(row).forEach(function(key) {
      if (headers.indexOf(key.trim()) === -1) {
        // console.log( 'putting new key ' + key.trim() + ' into headers ' + headers.toString());
        headers.push(key.trim());
      }
      let newValue = unmarshal(row[key]);

      if (typeof newValue === "object") {
        newRow[key] = JSON.stringify(newValue);
      } else {
        newRow[key] = newValue;
      }
    });

    // console.log(newRow);
    return newRow;
  });

  let endData = Papa.unparse([...rows], {
    newline: nl,
    header: scanIndex === 0,
    columns: [...headers],
  });

  return writeChunkToFile(endData);
}

if (program.describe) describeTable(query);
else scanDynamoDB(query);