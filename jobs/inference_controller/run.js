let nconf = require("nconf");
let azure = require("azure-storage");

let debug = true;

function tablify(o) {

  function add(r, k, v) {
    r[k] = { _ : v };
  }

  var row = {};
  for (var k in o) {
    if (typeof(o[k]) == 'object') {
      add(row, k, JSON.stringify(o[k]));
    }
    else {
      add(row, k, o[k]);
    }
  }

  return row;
}


function insertIteration(config, tableService, queueService, iteration, cb) {

  let jobs = [];
  let minPartition = 10;
  let maxPartition = 100;

  if (config.get('test')) {
    maxPartition = minPartition;
  }

  for (let partition = minPartition; partition <= maxPartition; partition++) {
    jobs.push({
      type : 'process',
      partition : partition,
      iteration : iteration
    });      
  }

  console.log("Starting iteration: " + iteration);

  let all = [];
  let queueName = config.get('command_queue');

  for (let job of jobs) {
    let p = new Promise((resolve, reject) => { 
      let row = {
        PartitionKey : 'status',
        RowKey : job.partition.toString(),
        iteration : job.iteration.toString(),
        status : 'ready'
      };

      tableService.insertOrReplaceEntity(
        config.get('command_table'), tablify(row), (err, result, response) => {
          if (err) {
            reject(err);
          }
          else {
            resolve(result);
          }
        }
      );
    });
    all.push();
  }

  Promise.all(all)
  .then(() => {
    all = [];
    for (let job of jobs) {
      let p = new Promise((resolve, reject) => {
        queueService.createMessage(queueName, JSON.stringify(job), (err, result) => {
          if (err) {
            reject(err);
          }
          else {
            resolve(result);
          }
        });
      });
      all.push();
    }
    
    Promise.all(all)
    .then(() => {
      cb();    
    })
    .catch((err) => { 
      throw err;
    });
  })
  .catch((err) => {
    console.error(err);
  });
}

function awaitIterationCompletion(config, tableService, queueService, iterations, generation) {

  let query = new azure.TableQuery().where("PartitionKey eq 'status'");

  tableService.queryEntities(config.get('command_table'), query, null, (err, result, response) => {
    if (err) {
      console.error(err);  
    }

    for (let row of result.entries) {
      console.log(row);
      if (row.status._ != 'processed') {
        setTimeout(() => {
          awaitIterationCompletion(config, tableService, queueService, iterations, generation);
        }, debug ? 5000 : 60 * 1000);
        return;
      }
    }

    iterations.shift();
    insertNextIteration(config, tableService, queueService, iterations, generation);
  });  
}

function insertNextIteration(config, tableService, queueService, iterations, generation) {
  insertIteration(config, tableService, queueService, iterations[0], () => {
    awaitIterationCompletion(config, tableService, queueService, iterations, generation);
  });
}

function main() {

  nconf.env().argv().defaults({config:'localConfig.json'});

  let configFile = nconf.get('config');
  let config = nconf.file({file:configFile, search:true});

  nconf.required(['table_storage_account', 'table_storage_key', 'command_table', 'command_queue']);
  let queueService = azure.createQueueService(
    config.get("table_storage_account"),
    config.get("table_storage_key")
  );
  queueService.messageEncoder = null

  let tableService = azure.createTableService(
    config.get("table_storage_account"),
    config.get("table_storage_key")
  );

  tableService.createTableIfNotExists(config.get('command_table'), (err, result) => {

    if (err) {
      console.warn('create command table');
      console.warn(err.stack);
    }

    queueService.createQueueIfNotExists(config.get('command_queue'), (err, result) => {

      if (err) {
        console.warn('create command queue');
        console.warn(err.stack);
      }

      let iterations = [];
      if (config.get('test')) {
        iterations = [1, 2];
      }
      else {
        for (let i = 1; i <= 5; i++) {
          iterations.push(i);
        }
      }

      let generation = "randomnumber";
      insertNextIteration(config, tableService, queueService, iterations, generation);
    });
  });
}

if (require.main === module) {
  main();
}
