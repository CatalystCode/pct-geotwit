var azure = require("azure");
var nconf = require("nconf");
var geolib = require("geolib");
var table = require("azure-storage-tools").table;

require("https").globalAgent.maxSockets = 128;

var config = nconf.env().file({ file: '../../localConfig.json' });
var TABLE = config.get("USER_TABLE");

var processed = 0;
var inferences = 0;

function writeFinalLocation(tableService, user, location, confidence, cb) {
  var mergeUser = {
    PartitionKey : { "_" : user.PartitionKey },
    RowKey : { "_" : user.RowKey },
    location : {"_" : JSON.stringify({
      latitude : location.latitude,
      longitude : location.longitude,
      confidence : confidence
    })}
  };

  tableService.mergeEntity(TABLE, mergeUser, (err, result) => {
    if (cb)
      cb(err, result);
  });
}

function clearInferredLocation(tableService, user, cb) {

  var mergeUser = {
    PartitionKey : { "_" : user.PartitionKey },
    RowKey : { "_" : user.RowKey },
    inferred_location : { "_" : JSON.stringify([]) }
  };

  tableService.mergeEntity(TABLE, mergeUser, (err, result) => {
    if (!err) {
    }
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
    if ("inferred_location" in user) {
      inferred_location = JSON.parse(user.inferred_location);
    }
    else {
      inferred_location = [];
    }
  }

  inferred_location.push({
    latitude : location.latitude,
    longitude : location.longitude
  });

  var mergeUser = {
    PartitionKey : { "_" : user.PartitionKey },
    RowKey : { "_" : user.RowKey },
    inferred_location : { "_" : JSON.stringify(inferred_location) }
  };

  tableService.mergeEntity(TABLE, mergeUser, (err, result) => {
    if (!err) {
      if (inferred_location.length == 1) {
        inferences++;
      }
    }
    if (cb)
      cb(err, result);
  });
}

function retryOrResolve(resolve, fn) {
  fn((err, result) => {
    if (err) {
      setTimeout(() => { retryOrResolve(resolve, fn); }, 10000 * Math.random());
    }
    else {
      resolve(result);
    }
  });
}

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

      retryOrResolve(resolve, (cb) => {
        tableService.queryEntities(TABLE, query, null, cb);
      });
    }));
  }

  return Promise.all(all);
}

function processUser(tableService, user, iteration) {

  processed++;

  return new Promise((resolve, reject) => {

    var locations = JSON.parse(user.locations);
    if (locations.length > 0) {
      resolve(true);
    }
    else {
      // User has never reported a location, infer one
      // from any neighbours
      getNeighboursLocations(tableService, user)
      .then((results) => {
        var latlons = [];
        if (results.length > 0) {
          for (var result of results) {
            if (result && result.entries.length > 0) {
              var loc;
              if (iteration == 1) {
                // First iteration, we only average over known locations
                // (confidence == 1)
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
                latlons.push({latitude:loc.latitude, longitude:loc.longitude});
              }
            }
          }
        }
        return latlons;
      })
      .then((latlons) => {
        if (latlons.length > 0) {
          var centrePoint = geolib.getCenter(latlons);
          if (iteration < 5) {
            retryOrResolve(resolve, (cb) => {
              writeInferredLocation(tableService, user, centrePoint, iteration, cb);
            });
          }
          else {
            retryOrResolve(resolve, (cb) => {
              writeFinalLocation(tableService, user, centrePoint, 0.5, cb);
            });
          }
        }
        else {
          if (iteration == 1) {
            retryOrResolve(resolve, (cb) => {
              clearInferredLocation(tableService, user, cb);
            });
          }
          else {
            resolve(true);
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
  });
}

function processPartition(tableService, partition, iteration, cb) {

  var query = new azure.TableQuery().where("PartitionKey == ?", partition);

  function processBatch(tableService, entries) {
    var all = [];
    for (var entry of entries) {
      var user = table.detablify(entry);
      all.push(processUser(tableService, user, iteration));
    }
    return Promise.all(all);
  }

  function nextBatch(continuationToken) {
    tableService.queryEntities(TABLE, query, continuationToken, (err, result) => {

      if (err) {
        console.warn("processPartition");
        console.warn(err.stack);
        setTimeout(() => {
          nextBatch(continuationToken);
        }, 5000);
        return;
      }

      processBatch(tableService, result.entries)
      .then((results) => {
        if (result.continuationToken) {
          process.nextTick(() => {
            nextBatch(result.continuationToken);
          });
        }
        else {
          cb();
        }
      })
      .catch((err) => {
        console.log("processBatch");
        console.warn(err.stack);
      });
    });
  };

  nextBatch(null);
}


function main() {

  // Add a timestamp that the Azure DataFactory can actually read

  var tableService = azure.createTableService(
    config.get("STORAGE_ACCOUNT"),
    config.get("STORAGE_KEY")
  );

  var queueService = azure.createQueueService(
    config.get("STORAGE_ACCOUNT"),
    config.get("STORAGE_KEY")
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

    console.log("Processed:" + processed);
    console.log("Inferences:" + inferences);

    processed = 0;
    inferences = 0;

    if (iterations.length > 0) {
      nextIteration(iterations.shift(), onIterationComplete);
    }
    else {
      var end = Date.now();
      console.log("done");
      console.log(end - start + "ms");
    }
  }

  var start = Date.now();
  nextIteration(iterations.shift(), onIterationComplete);
}

if (require.main === module) {
    main();
}
