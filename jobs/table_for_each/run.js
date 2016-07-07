var azure = require("azure");
var nconf = require("nconf");
var azure_storage_tools = require("../../pct-webjobtemplate/lib/azure-storage-tools");

var TABLE = "users";
var config = nconf.env().file({ file: '../../localConfig.json' });

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

/*function foreach(tableService, user) {

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
}*/

function foreach(tableService, user) {

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

  console.log(azure_storage_tools);

  var tableService = azure.createTableService(
    config.get("AZURE_STORAGE_ACCOUNT"),
    config.get("AZURE_STORAGE_ACCESS_KEY")
  );

  function processEntry(e) {
    //console.log(e);
  }

  var start = Date.now();
  azure_storage_tools.table.forEach(tableService, TABLE, processEntry, (err, result) => {
    if (err) {
      console.warn(err.stack);
      process.exit(1);
    }
    console.log("Processed: " + result + " entries");
    console.log((Date.now() - start) + "ms");
  });
}

if (require.main === module) {
    main();
}
