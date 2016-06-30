var azure = require("azure");
var nconf = require("nconf");
var geolib = require("geolib");
var Promise = require("Bluebird");

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

var processed = 0;
var reachable = 0;
var currentPartition = 0;

function writeUserPosition(tableService, user, position, confidence, cb) {
  var mergeUser = {
    PartitionKey : { "_" : user.PartitionKey },
    RowKey : { "_" : user.RowKey },
    location : {"_" : JSON.stringify({
      lat : position.latitude,
      lon : position.longitude,
      confidence : confidence
    })}
  };

  tableService.mergeEntity(TABLE, mergeUser, (err, result) => {
    if (cb)
      cb(err, result);
  });
}

function processUser(tableService, user) {

  var repliedTo = JSON.parse(user.replied_to);
  var repliedBy = JSON.parse(user.replied_by);

  processed++;
  currentPartition++;
  if (repliedTo.length > 0 || repliedBy.length > 0) {
    reachable++;
  }
  return;

  var locations = JSON.parse(user.locations);

  var locs = [];
  for (var location of locations) {
    var latlons = [];
    if ("place" in location) {
      var place = JSON.parse(location.place);
      if (place.bounding_box.type == "Polygon") {
        var coords = place.bounding_box.coordinates[0];
        for (var latlon of coords) {
          latlons.push({latitude:latlon[1], longitude:latlon[0]});
        }
      }
      else {
        console.warn("unknown bounding box type");
      }
    }
    else if ("geo" in location) {
      var geo = JSON.parse(location.geo);
      if (geo["type"] == "Point") {
        latlons.push({latitude:geo.coordinates[0], longitude:geo.coordinates[1]});
      }
      else {
        console.warn("unknown geo type");
        console.warn(geo);
      }
    }
    if (latlons.length > 0) {
      // Locs is all the locations this user has ever reported
      locs.push(geolib.getCenter(latlons));
    }
  }

  // We position the user at the centre point of all
  // the locations they've ever reported
  if (locs.length > 0) {
    var position = geolib.getCenter(latlons);
    writeUserPosition(tableService, user, position, 1, (err, result) => {
      if (err) {
        console.warn(err);
      }
    });
  }
}

function processBatch(tableService, entries) {
  for (var entry of entries) {
    var user = detablify(entry);
    processUser(tableService, user);
  }
}

function processPartition(tableService, partition, cb) {

  currentPartition = 0;
  console.log("Processing " + partition);

  var query = new azure.TableQuery().where("PartitionKey == ?", partition);

  function nextBatch(continuationToken) {
    tableService.queryEntities(TABLE, query, continuationToken, (err, result) => {
      if (err) {
        console.warn(err);
        process.exit(1);
      }
      processBatch(tableService, result.entries);
      if (result.continuationToken) {
        process.nextTick(() => {
          nextBatch(result.continuationToken);
        });
      }
      else {
        console.log("Processed: " + processed);
        console.log("Reachable: " + reachable);
        console.log("This Partition: " + currentPartition);
        cb();
      }
    });
  }

  nextBatch(null);
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

  var partitions = [];
  for (var partition = 10; partition < 100; partition++) {
    partitions.push(partition.toString());
  }

  function nextPartition() {
    processPartition(tableService, partitions[0], () => {
      partitions.shift();
      nextPartition();
    });
  }

  nextPartition();
}

if (require.main === module) {
    main();
}
