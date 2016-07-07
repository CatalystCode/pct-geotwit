var azure = require("azure");
var nconf = require("nconf");
var table = require("../../pct-webjobtemplate/lib/azure-storage-tools").table;

var TABLE = "tweets";
var QUEUE = "tweetsq";
var config = nconf.env().file({ file: '../../localConfig.json' });

function main() {

  var tableService = azure.createTableService(
    config.get("AZURE_STORAGE_ACCOUNT"),
    config.get("AZURE_STORAGE_ACCESS_KEY")
  );

  var queueService = azure.createQueueService(
    config.get("AZURE_STORAGE_ACCOUNT"),
    config.get("AZURE_STORAGE_ACCESS_KEY")
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
