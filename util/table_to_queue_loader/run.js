'use strict';

var azure = require("azure");
var nconf = require("nconf");

var config = nconf.env().argv();
var TABLE = config.get('table');
var QUEUE = config.get('queue');

function main() {

  var tableService = azure.createTableService(
    config.get("table_account_name"),
    config.get("table_account_key")
  );

  var queueService = azure.createQueueService(
    config.get("queue_account_name"),
    config.get("queue_account_key")
  );

  var complete = 0;
  function enqueueMessage(queueService, msg) {
    queueService.createMessage(QUEUE, msg, (err, result) => {
      if (err) {
        setTimeout(() => {
          enqueueMessage(queueService, msg);
        }, 5000);
      }
      else {
        complete++;
      }
    });
  };

  queueService.createQueueIfNotExists(QUEUE, (err, result) => {
    if (err) {
      console.warn(err.stack);
      process.exit(1);
    }

    table.forEach(tableService, TABLE,
      (e) => {
        enqueueMessage(queueService, JSON.stringify(table.detablify(e)));
      },
      (err, result) => {
        if (err) {
          console.warn(err.stack);
        }
        else {
          console.log("done");
          console.log(result + " entries");
        }
      }
    );
  });
}

if (require.main === module) {
    main();
}
