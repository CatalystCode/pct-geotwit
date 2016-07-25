var azure = require("azure");
var nconf = require("nconf");
var EventHubClient = require("azure-event-hubs").Client;
var table = require("../../pct-webjobtemplate/lib/azure-storage-tools").table;

var config = nconf.env().file({ file: '../../localConfig.json' });
var TABLE = config.get("TWEET_TABLE_NAME");

function main() {

  var tableService = azure.createTableService(
    config.get("STORAGE_ACCOUNT"),
    config.get("STORAGE_KEY")
  );

  var eventHubSender = null;
  var eventHubClient = EventHubClient.fromConnectionString(
    config.get("twitter_to_location_write_config")
  );

  eventHubClient.createSender()
  .then((tx) => {

    tx.on("errorReceived", (err) => {
      console.warn("eventhubsender: " + err);
    });

    table.forEach(tableService, TABLE,
      (e) => {
        tx.send(table.detablify(e));
      },
      (err, result) => {
        if (err) {
          console.warn(err.stack);
        }
        else {
          console.log("done.");
          console.log(result + " entries");
        }
      }
    );
  });
}

if (require.main === module) {
    main();
}
