var azure = require("azure");
var nconf = require("nconf");
var Promise = require("Bluebird");

var TABLE = "tweets";
var config = nconf.env().file({ file: '../../localConfig.json' });
var EventProcessorHost = require("./lib/EventProcessorHost.js");

function onMessage(msg) {
  console.log(msg);
}

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
    return eph.registerEventProcessor("$Default", null);
  })
  .catch((e) => {
    console.warn("main: ");
    console.warn(e.stack);
    process.exit(1);
  });
}

if (require.main === module) {
    main();
}
