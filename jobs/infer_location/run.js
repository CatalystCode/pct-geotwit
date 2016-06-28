var azure = require("azure");
var nconf = require("nconf");
var Promise = require("Bluebird");

var USER_TABLE = "users";
var config = nconf.env().file({ file: '../../localConfig.json' });
var EventProcessorHost = require("./lib/EventProcessorHost.js");

function onMessage(tableService, msg) {

  function add(r, k, v) {
    r[k] = { _ : v };
  }

  function writeUser(u) {

    var row = {};
    for (var k in u) {
      if (typeof(u[k]) == 'object') {
        add(row, k, JSON.stringify(u[k]));
      }
      else {
        add(row, k, u[k].toString());
      }
    }

    tableService.insertOrReplaceEntity(USER_TABLE, row, (err, result) => {
      if (err) {
        setTimeout(() => { writeUser(row); }, 100);
      }
    });
  }

  var userId = msg.user_id._.toString();
  var partKey = userId.slice(0, 2);

  var tableQuery = new azure.TableQuery().where("PartitionKey == ?", partKey)
  .and("RowKey == ?", userId);

  tableService.queryEntities(USER_TABLE, tableQuery, null, (err, result) => {

    if (err) {
      console.warn(err);
      return;
    }

    var user = {
      PartitionKey : partKey,
      RowKey : userId
    };

    if (result.entries.length == 0) {
      // New user
      user.locations = [];
      user.replied_to = [];
    }
    else {
      // Existing user
      var entry = result.entries[0];
      user.locations = JSON.parse(entry.locations._);
      user.replied_to = JSON.parse(entry.replied_to._);
    }

    console.log(msg);

    var update = false;
    if ("in_reply_to" in msg) {
      update = true;
      user.replied_to.push(parseInt(msg.in_reply_to._));
    }

    if ("geo" in msg && msg.geo._ != 'null') {
      update = true;
      user.locations.push({"geo":msg.geo._});
    }

    if ("place" in msg && msg.place._ != 'null') {
      update = true;
      user.locations.push({"place":msg.place._});
    }

    if (update) {
      writeUser(user);
    }
  });
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

  tableService.createTableIfNotExists(USER_TABLE, (err, result) => {

    var eph = new EventProcessorHost(blobService, config.get("twitter_to_location_read_config"));

    eph.init()
    .then(() => {
      return eph.registerEventProcessor("$Default", (msg) => {
        onMessage(tableService, msg.body);
      });
    })
    .catch((e) => {
      console.warn("main: ");
      console.warn(e.stack);
      process.exit(1);
    });

  });
}

if (require.main === module) {
    main();
}
