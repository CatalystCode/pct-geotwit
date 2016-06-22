var azure = require("azure");
var nconf = require("nconf");

var TABLE = "tweets";
var config = nconf.env().file({ file: '../../localConfig.json' });
var EventProcessorHost = require("./lib/EventProcessorHost.js");

function main() {

  // Add a timestamp that the Azure DataFactory can actually read

  var tableService = azure.createTableService(
    config.get("AZURE_STORAGE_ACCOUNT"),
    config.get("AZURE_STORAGE_ACCESS_KEY")
  );

  var blobService = azure.createBlobService(
    config.get("AZURE_STORAGE_ACCOUNT"),
    config.get("AZURE_STORAGE_ACCESS_KEY")
  );

  var eph = new EventProcessorHost(blobService, config.get("twitter_to_location_read_config"));
  eph.init()
  .then(() => {
    console.log("done");
  })
  .catch((e) => {
    console.warn(e.stack);
    process.exit(1);
  });

/*
  function processMessages(partitions) {

    acquirePartition(currentPartitions, partitions)
    .then((partition) => {
      console.log("Acquired");
      return eventHubClient.createReceiver('$Default', partition);
    })
    .then((rx) => {
      function onError(err) {
        console.warn("eventhubreceiver: " + err);
      }

      function onMessage(msg) {
        console.log(msg);
      }

      var interval = setInterval(() => {
        renewPartition(partition)
        .then((result) => {
          if (result) {
            // We succesfully renewed the lease
          }
          else {
            clearInterval(interval);
            console.log("lease broken: " + partition);
            rx.removeListener(onError);
            rx.removeListener(onMessage);
            checkpoint();
            releasePartition(partition)
            .then(() => {
              // All done with this partition (for now)
              return;
            });
          }
        });
      }, 30000);

      rx.on("errorReceived", onError);
      rx.on('message', onMessage);
    })
    .catch((err) => {
      console.warn(err.stack);
      process.exit(1);
    });
  }

  var partitions = null;
  eventHubClient.getPartitionIds()
  .then((parts) => {
    partitions = parts;
    return initPartitionBlobs(blobService, container, blobRoot, partitions);
  })
  .then(() => {
    console.log("Processing");
    processMessages(partitions);
  })
  .catch((err) => {
    console.warn(err.stack);
    process.exit(1);
  });*/
}

if (require.main === module) {
    main();
}
