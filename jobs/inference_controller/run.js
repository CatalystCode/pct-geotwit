let nconf = require("nconf");
let azure = require("azure-storage");

function insertIteration(config, queueService, iteration, cb) {

  let jobs = [];
  let minPartition = 10;
  let maxPartition = 100;

  if (config.get('test')) {
    maxPartition = minPartition;
  }

  for (let part = minPartition; part <= maxPartition; part++) {
    jobs.push({
      type : 'process',
      partition : part,
      iteration : iteration
    });      
  }

  console.log(jobs);

  let queueName = config.get('command_queue');
  for (let job of jobs) {
    queueService.createMessage(queueName, JSON.stringify(job), (err, result) => {
      if (err) {
        console.warn(err);
        cb(err);
      }
      else {
        cb(null);
      }
    });
  };
}

function awaitIterationCompletion(config, tableService, queueService, iterations) {

  
  tableService.
}

function insertNextIteration(config, tableService, queueService, generation, iterations) {
  insertIteration(config, queueService, iterations[0], () => {
    awaitIterationCompletion(config, queueService, iterations);
  });
}

function main() {

  nconf.env().argv().defaults({config:'localConfig.json'});

  let configFile = nconf.get('config');
  let config = nconf.file({file:configFile, search:true});

  nconf.required(['table_storage_account', 'table_storage_key', 'command']);
  let queueService = azure.createQueueService(
    config.get("table_storage_account"),
    config.get("table_storage_key")
  );
  queueService.messageEncoder = null

  tableService.createTableIfNotExists(config.get('command'), (err, result) => {

    if (err) {
      console.warn('create command table');
      console.warn(err.stack);
    }

    queueService.createQueueIfNotExists(config.get('command'), (err, result) => {

      if (err) {
        console.warn('create command queue');
        console.warn(err.stack);
      }

      let iterations = [];
      if (config.get('test')) {
        iterations = [1, 2];
      }
      else {
        for (let i = 0; i <= 5; i++) {
          iterations.push(i);
        }
      }

      let generation = "randomnumber";
      insertNextIteration(config, tableService, queueService, generation, iterations);
    });
  });
}

if (require.main === module) {
  main();
}
