var azure = require("azure");
var nconf = require("nconf");
var azure_storage = require("azure-storage");

var TABLE = "tweets";
var config = nconf.env().file({ file: '../../localConfig.json' });

function main() {

  // Add a timestamp that the Azure DataFactory can actually read

  var tableService = azure.createTableService(
    config.get("AZURE_STORAGE_ACCOUNT"),
    config.get("AZURE_STORAGE_ACCESS_KEY")
  );

  var complete = 0;
  function processBatch(entries) {

    var batches = [];
    batches.unshift(new azure_storage.TableBatch());

    for (var entry of entries) {
      if (batches[0].size() == 100) {
        batches.unshift(new azure_storage.TableBatch());
      }
      entry.isotimestamp = { '_' : new Date(parseInt(entry.timestamp._)) };
      batches[0].mergeEntity(entry);
    }

    for (var batch of batches) {
      tableService.executeBatch(TABLE, batch, (err, result) => {
        if (err) {
          console.log("batch: " + err);
        }
        complete += batch.size();
      });
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

  nextBatch(null);
}

if (require.main === module) {
    main();
}
