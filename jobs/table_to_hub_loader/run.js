var azure = require("azure");
var nconf = require("nconf");
var EventHubClient = require("azure-event-hubs").Client;

var TABLE = "tweets";
var config = nconf.env().file({ file: '../../localConfig.json' });

function main() {

  var tableService = azure.createTableService(
    config.get("AZURE_STORAGE_ACCOUNT"),
    config.get("AZURE_STORAGE_ACCESS_KEY")
  );

  var eventHubSender = null;
  var eventHubClient = EventHubClient.fromConnectionString(
    config.get("twitter_to_location_write_config")
  );

  var complete = 0;
  function processBatch(entries) {
    for (var entry of entries) {
      eventHubSender.send(entry);
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

  eventHubClient.createSender()
  .then((tx) => {
    eventHubSender = tx;
    eventHubSender.on("errorReceived", (err) => {
      console.warn("eventhubsender: " + err);
    });
    nextBatch(null);
  });

}

if (require.main === module) {
    main();
}
