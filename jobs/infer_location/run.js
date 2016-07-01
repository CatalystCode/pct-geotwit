var azure = require("azure");
var nconf = require("nconf");
var geolib = require("geolib");

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

function writeFinalLocation(tableService, user, location, confidence, cb) {
  var mergeUser = {
    PartitionKey : { "_" : user.PartitionKey },
    RowKey : { "_" : user.RowKey },
    location : {"_" : JSON.stringify({
      lat : location.latitude,
      lon : location.longitude,
      confidence : confidence
    })}
  };

  updated++;

  tableService.mergeEntity(TABLE, mergeUser, (err, result) => {
    if (cb)
      cb(err, result);
  });
}

function writeInferredLocation(tableService, user, location, iteration, cb) {

  var inferred_location;
  if (iteration == 1) {
    inferred_location = [];
  }
  else {
    inferred_location = JSON.parse(user.inferred_location._);
  }

  inferred_location.push(JSON.stringify([{
    lat : location.latitude,
    lon : location.longitude
  }]));

  var mergeUser = {
    PartitionKey : { "_" : user.PartitionKey },
    RowKey : { "_" : user.RowKey },
    inferred_location : { "_" : inferred_location }
  };

  updated++;

  tableService.mergeEntity(TABLE, mergeUser, (err, result) => {
    if (cb)
      cb(err, result);
  });
}


var userCache = {};

function getNeighboursLocations(tableService, user) {

  var repliedTo = JSON.parse(user.replied_to);
  var repliedBy = JSON.parse(user.replied_by);

  var neighbours = new Set(repliedTo.concat(repliedBy));

  var all = [];
  for (var neighbour of neighbours) {

    all.push(new Promise((resolve, reject) => {
      var partKey = neighbour.slice(0, 2);
      var rowKey = neighbour;

      var query = new azure.TableQuery().select(["location", "inferred_location"])
      .where("PartitionKey == ?", partKey).and("RowKey == ?", neighbour);

      tableService.queryEntities(TABLE, query, null, (err, result) => {
        if (err) {
          console.warn("getNeighbours");
          console.warn(err.stack);
          resolve(null);
        }
        else {
          resolve(result);
        }
      });
    }));
  }

  return Promise.all(all);
}

function processUser(tableService, user, iteration) {

  processed++;

  var latlons = [];
  var locations = JSON.parse(user.locations);
  if (!locations.length) {
    // User has never reported a location, infer one
    // from any neighbours
    getNeighboursLocations(tableService, user)
    .then((results) => {
      if (results.length > 0) {
        for (var result of results) {
          if (result) {

            var loc;
            if (iteration == 1) {
              loc = JSON.parse(result.entries[0].location._);
              if (loc.confidence < 1) {
                loc = null;
              }
            }
            else {
              var inferred_locations = JSON.parse(result.entries[0].inferred_location._);
              if (inferred_locations && inferred_locations.length > 0) {
                loc = inferred_locations[inferred_locations.length - 1];
              }
              else {
                loc = JSON.parse(result.entries[0].location._);
              }
            }

            if (loc) {
              latlons.push({latitude:loc.lat, longitude:loc.lon});
            }
          }
        }
      }
    })
    .then(() => {
      if (latlons.length > 0) {
        var centrePoint = geolib.getCenter(latlons);
        if (iteration < 5) {
          writeInferredLocation(tableService, user, centrePoint, iteration);
        }
        else {
          writeFinalLocation(tableService, user, centrePoint, 0.5);
        }
      }
    })
    .catch((err) => {
      console.warn("processUser");
      console.warn(err.stack);
      setTimeout(() => {
       processUser(tableService, user, iteration);
      }, 5000);
    });
  }

}

function processBatch(tableService, entries, minConfidence) {
  for (var entry of entries) {
    var user = detablify(entry);
    processUser(tableService, user, minConfidence);
  }
}

function processPartition(tableService, partition, minConfidence, cb) {

  console.log("Processing " + partition);

  var query = new azure.TableQuery().where("PartitionKey == ?", partition);

  function nextBatch(continuationToken) {
    tableService.queryEntities(TABLE, query, continuationToken, (err, result) => {
      if (err) {
        console.warn("processPartition");
        console.warn(err.stack);
        process.exit(1);
      }
      processBatch(tableService, result.entries, minConfidence);
      if (result.continuationToken) {
        process.nextTick(() => {
          nextBatch(result.continuationToken);
        });
      }
      else {
        console.log("Processed: " + processed);
        console.log("Updated: " + updated);
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


  function nextPartition(partition, iteration, cb) {
    console.log("Partition: " + partition);
    processPartition(tableService, partition, iteration, cb);
  }

  function nextIteration(iteration, cb) {

    console.log("Iteration: " + iteration);

    var partitions = [];
    for (var partition = 10; partition < 100; partition++) {
      partitions.push(partition.toString());
    }

    function onPartitionComplete() {
      if (partitions.length > 0) {
        nextPartition(partitions.shift(), iteration, onPartitionComplete);
      }
      else {
        cb();
      }
    }

    nextPartition(partitions.shift(), iteration, onPartitionComplete);
  }

  var iterations = [1, 2, 3, 4, 5];
  function onIterationComplete() {
    if (iterations.length > 0) {
      nextIteration(iterations.shift(), onIterationComplete);
    }
  }

  nextIteration(iterations.shift(), onIterationComplete);
}

if (require.main === module) {
    main();
}
