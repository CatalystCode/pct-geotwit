var azure = require("azure");
var nconf = require("nconf");

var TABLE = "users";
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

var updated = 0;
var processed = 0;

var writeQueue = [];
var errorQueue = [];
var outstandingWrites = 0;
var MAX_PARALLEL_WRITES = 100;

function pumpWriteQueue(tableService) {

  while (writeQueue.length > 0 && outstandingWrites < MAX_PARALLEL_WRITES) {

    outstandingWrites++;
    var entry = writeQueue.shift();

    tableService.insertOrReplaceEntity(TABLE, entry, (err, result) => {

      if (err) {
        errorQueue.push(entry);
        console.warn(err);
      }

      updated++;
      outstandingWrites--;

      process.nextTick(() => {
        pumpWriteQueue(tableService);
      });
    });
  }
}

function writeUser(tableService, u) {

  function add(r, k, v) {
    r[k] = { _ : v };
  }

  var row = {};
  for (var k in u) {
    if (typeof(u[k]) == 'object') {
      add(row, k, JSON.stringify(u[k]));
    }
    else {
      add(row, k, u[k]);
    }
  }

  writeQueue.push(row);
  pumpWriteQueue(tableService);
}

function foreach(tableService, user) {

  var update = false;
  var location = JSON.parse(user.location);

  if (location.lat != undefined) {
    update = true;
    location.latitude = location.lat;
    delete location.lat;
  }
  if (location.lon != undefined) {
    update = true;
    location.longitude = location.lon;
    delete location.lon;
  }

  processed++;
  if (update == true) {
    user.location = location;
    writeUser(tableService, user);
  }
}

/*
  var new_locations = [];
  var locations = JSON.parse(user.locations);

  var update = false;
  for (var location of locations) {
    if ("place" in location) {
      var place = location.place;
      if (typeof(place) == "string") {
        update = true;
        new_locations.push({"place" : JSON.parse(place)});
      }
    }
    else if ("geo" in location) {
      var geo = location.geo;
      if (typeof(geo) == "string") {
        update = true;
        new_locations.push({"geo" : JSON.parse(geo)});
      }

    }
  }

  if (update) {
    user.locations = new_locations;
    writeUser(tableService, user);
  }
  processed++;
}*/

function main() {

  var tableService = azure.createTableService(
    config.get("AZURE_STORAGE_ACCOUNT"),
    config.get("AZURE_STORAGE_ACCESS_KEY")
  );

  function processBatch(entries) {
    for (var entry of entries) {
      foreach(tableService, detablify(entry));
    }
  }

  function nextBatch(continuationToken) {
    tableService.queryEntities(TABLE, null, continuationToken, (err, result) => {
      if (err) {
        console.warn("query");
        console.warn(err);
        setTimeout(() => {
          nextBatch(continuationToken);
        }, 500);
        return;
      }

      processBatch(result.entries);

      if (result.continuationToken) {
        process.nextTick(() => {
          nextBatch(result.continuationToken);
          console.log("Processed: " + processed);
          console.log("Updated: " + updated);
          console.log("WriteQueue: " + writeQueue.length);
        });
      }
      else {
        console.log("done");
      }

    });
  }
  nextBatch(null);
}

if (require.main === module) {
    main();
}
