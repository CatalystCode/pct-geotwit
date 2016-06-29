var azure = require("azure");
var nconf = require("nconf");

var TABLE = "tweets";
var QUEUE = "tweetsq";
var config = nconf.env().file({ file: '../../localConfig.json' });

function detablify(t) {
  var o = {};
  for (var k in t) {
    if (k[0] != '.') {
      o[k] = t[k]._;
    }
  }
  return o;
}

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

  function processBatch(entries) {
    for (var entry of entries) {
      var msg = JSON.stringify(detablify(entry));
      enqueueMessage(queueService, msg);
    }
  }

  function nextBatch(continuationToken) {
    tableService.queryEntities(TABLE, null, continuationToken, (err, result) => {
      if (err) {
        console.warn(err);
        process.exit(1);
      }
      processBatch(result.entries);
      if (result.continuationToken) {
        process.nextTick(() => {
          nextBatch(result.continuationToken);
        });
      }
      else {
        console.log("Complete: " + complete);
      }
    });
  }

  queueService.createQueueIfNotExists(QUEUE, (err, result) => {
    if (err) {
      console.warn("createQueue");
      console.warn(err);
      process.exit(1);
    }
    nextBatch(null);
  });
}

if (require.main === module) {
    main();
}
