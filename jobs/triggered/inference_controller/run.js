'use strict';

let nconf = require("nconf");
let azure = require("azure-storage");

require('https').globalAgent.maxSockets = 16;

let _debug = true;

function debug(msg) {
  if (_debug && msg) {
    if (msg.stack !== undefined) {
      console.log(msg.stack);
    }
    else {
      console.log(msg);
    }
  }
}

function tablify(o) {

  function add(r, k, v) {
    r[k] = { _ : v };
  }

  var row = {};
  for (var k in o) {
    if (typeof(o[k]) === 'object') {
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
  config.reque
  let minPartition = config.get("startPartition");
  let maxPartition = config.get("endPartition");

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
        config.get('command_table'), tablify(row), (err, result) => {
          if (err) {
            reject(err);
          }
          else {
            resolve(result);
          }
        }
      );
    });
    all.push(p);
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
      all.push(p);
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

  tableService.queryEntities(config.get('command_table'), query, null, (err, result) => {

    if (err) {
      console.error(err.stack);  
    }

    for (let row of result.entries) {
      if (row.status._ !== 'processed') {
        setTimeout(() => {
          awaitIterationCompletion(config, tableService, queueService, iterations, generation);
        }, _debug ? 5000 : 60 * 1000);
        return;
      }
    }

    iterations.shift();
    if (iterations.length > 0) {
      insertNextIteration(config, tableService, queueService, iterations, generation);
    }
    else {
      console.log("All done, exiting");
    }
  });  
}

function insertNextIteration(config, tableService, queueService, iterations, generation) {
  insertIteration(config, tableService, queueService, iterations[0], () => {
    awaitIterationCompletion(config, tableService, queueService, iterations, generation);
  });
}

function init(config, tableService, queueService, cb) {

  let query = new azure.TableQuery().where('PartitionKey eq \'status\'');

  tableService.queryEntities(config.get('command_table'), query, null, (err, result) => {

    if (err) {
      console.error(err.stack);
    }

    let all = [];

    for (let entry of result.entries) {
      let entity = {
        PartitionKey : { _ : 'status'},
        RowKey : { _ : entry.RowKey._ }
      };

      all.push(new Promise((resolve, reject) => {      
        tableService.deleteEntity(config.get('command_table'), entity, (err) => {
          if (err) {
            console.error(err.stack);
            resolve(false);
          }
          else {
            resolve(true);
          }
        });
      }));
    }

    Promise.all(all)
    .then((results) => {
      queueService.clearMessages(config.get('command_queue'), (err) => {
        if (err) {
          console.error(err.stack);
        }
        cb();
      });
    });
  });
}

function main() {

  nconf.env().argv().defaults({
    config : 'localConfig.json',
    startPartition : 10,
    endPartition : 99
  });

  let configFile = nconf.get('config');
  let config = nconf.file({file:configFile, search:true});

  nconf.required(['table_storage_account', 'table_storage_key', 'command_table', 'command_queue']);
  let queueService = azure.createQueueService(
    config.get("table_storage_account"),
    config.get("table_storage_key")
  );
  queueService.messageEncoder = null;

  let tableService = azure.createTableService(
    config.get("table_storage_account"),
    config.get("table_storage_key")
  );

  tableService.createTableIfNotExists(config.get('command_table'), (err) => {

    debug(err);

    queueService.createQueueIfNotExists(config.get('command_queue'), (err) => {

      if (err) {
        console.warn('create command queue');
        console.warn(err.stack);
      }

      init(config, tableService, queueService, (err) => {

        debug(err);

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
  });
}

if (require.main === module) {
  main();
}
