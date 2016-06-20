var azure = require("azure");
var nconf = require("nconf");
var azure_storage = require("azure-storage");

var SOURCE_TABLE = "tweets";
var config = nconf.env().file({ file: '../../localConfig.json' });

function main() {

  // Add a timestamp that the Azure DataFactory can actually read

  var tableService = azure.createTableService(
    config.get("AZURE_STORAGE_ACCOUNT"),
    config.get("AZURE_STORAGE_ACCESS_KEY")
  );

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

    console.log(row);
    tableService.insertOrReplaceEntity("users", row, (err, result) => {
      if (err) {
        console.warn("insert: " + err);
      }
    });
  }

  var users = {};
  function processBatch(entries) {
    for (var entry of entries) {

      var update = false;
      var user_id = entry.user_id._;

      if (!(user_id in users)) {

        update = true;

        users[user_id] = {
          PartitionKey : (parseInt(user_id) % 128).toString(),
          RowKey : parseInt(user_id).toString(),
          screen_name : entry.user_screen_name._,
          locations : [],
          replied_to: []
        };
      }

      var user = users[user_id];

      if ("in_reply_to" in entry) {
        update = true;
        user.replied_to.push(entry.in_reply_to._);
      }

      if ("geo" in entry && entry.geo._ != 'null') {
        update = true;
        user.locations.push({"geo":entry.geo._});
      }

      if ("place" in entry && entry.place._ != 'null') {
        update = true;
        user.locations.push({"place":entry.place._});
      }

      if (update) {
        writeUser(user);
      }
    }
  }

  function nextBatch(continuationToken) {
    tableService.queryEntities(SOURCE_TABLE, null, continuationToken, (err, result) => {
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
        console.log("Complete: " + Object.keys(users).length);
      }
    });
  }

  nextBatch(null);
}

if (require.main === module) {
    main();
}
