var azure = require("azure");
var nconf = require("nconf");

var QUEUE = "tweetsq";
var USER_TABLE = "users";

var config = nconf.env().file({ file: '../../localConfig.json' });

function random(low, high) {
  return Math.random() * (high - low) + low;
}

function writeUser(tableService, user, cb) {

  function add(r, k, v) {
    r[k] = { _ : v };
  }

  var row = {};
  for (var k in user) {
    if (typeof(user[k]) == 'object') {
      add(row, k, JSON.stringify(user[k]));
    }
    else {
      add(row, k, user[k].toString());
    }
  }

  tableService.insertOrReplaceEntity(USER_TABLE, row, (err, result) => {
    if (cb) {
      cb(err, result);
    }
  });
}

function updateRepliedBy(tableService, userId, repliedBy) {

  var partKey = userId.slice(0, 2);

  var user = {
    PartitionKey : partKey,
    RowKey : userId
  };

  var tableQuery = new azure.TableQuery().where("PartitionKey == ?", partKey)
  .and("RowKey == ?", userId);

  tableService.queryEntities(USER_TABLE, tableQuery, null, (err, result) => {

    if (err) {
      console.warn("updateRepliedBy");
      console.warn(err);
    }
    else {
      if (result.entries.length == 0) {
        // New user
        user.locations = [];
        user.replied_to = [];
        user.replied_by = [];
        user.location = {
          "lat" : random(-90, 90),
          "lon" : random(-180, 180),
          "confidence" : 0
        };
      }
      else {
        // Existing user
        var entry = result.entries[0];
        user.locations = JSON.parse(entry.locations._);
        user.replied_to = JSON.parse(entry.replied_to._);
        user.replied_by = JSON.parse(entry.replied_by._);
        user.location = JSON.parse(entry.location._);
      }
    }

    if (!(repliedBy in user.replied_by)) {
      user.replied_by.push(repliedBy);
      writeUser(tableService, user);
    }
  });
}

function processMessage(tableService, queueService, msg, cb) {

  var userId = msg.user_id.toString();
  var partKey = userId.slice(0, 2);

  var tableQuery = new azure.TableQuery().where("PartitionKey == ?", partKey)
  .and("RowKey == ?", userId);

  tableService.queryEntities(USER_TABLE, tableQuery, null, (err, result) => {

    if (err) {
      console.warn(err);
      return;
    }

    var update = false;

    var user = {
      PartitionKey : partKey,
      RowKey : userId
    };

    if (result.entries.length == 0) {
      // New user
      user.locations = [];
      user.replied_to = [];
      user.replied_by = [];
      user.location = {
        "lat" : random(-90, 90),
        "lon" : random(-180, 180),
        "confidence" : 0
      };
      update = true;
    }
    else {
      // Existing user
      var entry = result.entries[0];
      user.locations = JSON.parse(entry.locations._);
      user.replied_to = JSON.parse(entry.replied_to._);
      user.replied_by = JSON.parse(entry.replied_by._);
      user.location = JSON.parse(entry.location._);
    }

    if ("in_reply_to" in msg) {
      if (!(msg.in_reply_to in user.replied_to)) {
        update = true;
        repliedTo = msg.in_reply_to.toString();
        user.replied_to.push(repliedTo);
        updateRepliedBy(tableService, repliedTo, userId);
      }
    }

    if ("geo" in msg && msg.geo != 'null') {
      update = true;
      user.locations.push({"geo":msg.geo});
    }

    if ("place" in msg && msg.place != 'null') {
      update = true;
      user.locations.push({"place":msg.place});
    }

    if (update) {
      writeUser(tableService, user, cb);
    }
    else {
      cb(null, true);
    }
  });
}

function onMessage(tableService, queueService, message) {
  return new Promise((resolve, reject) => {
    processMessage(tableService, queueService, JSON.parse(message.messagetext), (err, result) => {
      if (err) {
        resolve([]);
      } else {
        resolve([message.messageid, message.popreceipt]);
      }
    });
  });
}

function pump(tableService, queueService) {

  var options = { numOfMessages:32 };
  queueService.getMessages(QUEUE, options, (err, result) => {
    if (!err) {
      Promise.all(result.map((msg) => onMessage(tableService, queueService, msg)))
      .then((results) => {
        for (var result of results) {
          if (result.length > 0) {
            queueService.deleteMessage(QUEUE, result[0], result[1], (err, result) => {
            });
          }
        }
        process.nextTick(() => {
          pump(tableService, queueService);
        });
      });
    }
    else {
      console.warn(err);
    }
  });
}

function main() {

  // Add a timestamp that the Azure DataFactory can actually read

  var tableService = azure.createTableService(
    config.get("AZURE_STORAGE_ACCOUNT"),
    config.get("AZURE_STORAGE_ACCESS_KEY")
  );

  var queueService = azure.createQueueService(
    config.get("AZURE_STORAGE_ACCOUNT"),
    config.get("AZURE_STORAGE_ACCESS_KEY")
  );

  tableService.createTableIfNotExists(USER_TABLE, (err, result) => {

    if (err) {
      console.warn(err);
      process.exit(1);
    }

    pump(tableService, queueService);
  });
}

if (require.main === module) {
    main();
}
