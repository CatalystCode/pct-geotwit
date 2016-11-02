'use strict';

let nconf = require('nconf');
let geolib = require('geolib');
let azure = require('azure-storage');

let https = require('https');
https.globalAgent = new https.Agent({keepAlive:true, maxSockets:64, maxFreeSockets:64});

let _debug = true;

function detablify(t) {
  var o = {};
  for (var k in t) {
    if (k[0] !== '.') {
      o[k] = t[k]._;
    }
  }
  return o;
}

function tablify(o) {

  function add(r, k, v) {
    r[k] = { _ : v };
  }

  var row = {};
  for (var k in o) {
    if (typeof(o[k]) === 'object') {
      add(row, k, JSON.stringify(o[k]));
    }
    else {
      add(row, k, o[k]);
    }
  }

  return row;
}

function logError(name, err) {
  console.error(name.bgRed);
  console.error(err.stack);
}

function timerStart(name) {
  console.time(name);
}

function timerEnd(name) {
  console.timeEnd(name);
}

function debug(msg) {
  if (msg) {
    console.log(msg);
  }
}
 
function writeFinalLocation(config, tableService, user, location, confidence, cb) {
  let mergeUser = {
    PartitionKey : { '_' : user.PartitionKey },
    RowKey : { '_' : user.RowKey },
    location : {'_' : JSON.stringify({
      latitude : location.latitude,
      longitude : location.longitude,
      confidence : confidence
    })}
  };

  tableService.mergeEntity(config.get('user_table'), mergeUser, (err, result) => {
    if (cb) {
      cb(err, result);
    }
  });
}

function clearInferredLocation(config, tableService, user, cb) {

  let mergeUser = {
    PartitionKey : { '_' : user.PartitionKey },
    RowKey : { '_' : user.RowKey },
    inferred_location : { '_' : JSON.stringify([]) }
  };

  tableService.mergeEntity(config.get('user_table'), mergeUser, (err, result) => {
    if (cb) {
      cb(err, result);
    }
  });
}

function writeInferredLocation(config, tableService, user, location, iteration, cb) {

  let inferred_location;
  if (iteration === 1) {
    inferred_location = [];
  }
  else {
    if ('inferred_location' in user) {
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

  let mergeUser = {
    PartitionKey : { '_' : user.PartitionKey },
    RowKey : { '_' : user.RowKey },
    inferred_location : { '_' : JSON.stringify(inferred_location) }
  };

  tableService.mergeEntity(config.get('user_table'), mergeUser, (err, result) => {
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

function getNeighboursLocations(config, tableService, user) {

  let repliedTo = JSON.parse(user.replied_to);
  let repliedBy = JSON.parse(user.replied_by);

  let neighbours = new Set(repliedTo.concat(repliedBy));

  function queryNeighbour(neighbour) {
    return new Promise((resolve) => {
      let partKey = neighbour.slice(0, 2);
      let rowKey = neighbour;

      let query = new azure.TableQuery().select(['location', 'inferred_location'])
      .where('PartitionKey == ?', partKey).and('RowKey == ?', rowKey);

      retryOrResolve(resolve, (cb) => {
        tableService.queryEntities(config.get('user_table'), query, null, cb);
      });
    });
  }

  let all = [];
  for (let neighbour of neighbours) {
    all.push(queryNeighbour(neighbour));
  }

  return Promise.all(all);
}

function processUser(config, tableService, user, iteration, stats) {

  return new Promise((resolve) => {

    stats.users++;
    let locations = JSON.parse(user.locations);
    if (locations.length > 0) {
      // User already has an absolute (reported) location, never change these
      stats.absoluteLocations++;
      resolve(true);
    }
    else {
      // User has never reported a location, infer one
      // from any neighbours
      getNeighboursLocations(config, tableService, user)
      .then((results) => {
        let latlons = [];
        if (results.length > 0) {
          for (let result of results) {
            if (result && result.entries.length > 0) {
              let loc;
              if (iteration === 1) {
                // First iteration, we only average over known locations
                // (confidence == 1)
                loc = JSON.parse(result.entries[0].location._);
                if (loc.confidence < 1) {
                  loc = null;
                }
              }
              else {
                let inferred_locations = JSON.parse(result.entries[0].inferred_location._);
                if (inferred_locations && inferred_locations.length > 0) {
                  loc = inferred_locations[inferred_locations.length - 1];
                }
                else {
                  loc = JSON.parse(result.entries[0].location._);
                }
              }

              if (loc) {
                if (loc.latitude !== undefined && loc.longitude !== undefined) {
                  latlons.push({latitude:loc.latitude, longitude:loc.longitude});
                }
              }
            }
          }
        }
        return latlons;
      })
      .then((latlons) => {

        if (latlons.length > 0) {
          let centrePoint = geolib.getCenter(latlons);
          if (iteration < 5) {
            retryOrResolve(resolve, (cb) => {
              stats.inferredLocations++;
              writeInferredLocation(config, tableService, user, centrePoint, iteration, cb);
            });
          }
          else {
            retryOrResolve(resolve, (cb) => {
              stats.finalLocations++;
              writeFinalLocation(config, tableService, user, centrePoint, 0.5, cb);
            });
          }
        }
        else {
          if (iteration === 1) {
            retryOrResolve(resolve, (cb) => {
              clearInferredLocation(config, tableService, user, cb);
            });
          }
          else {
            resolve(true);
          }
        }
      })
      .catch((err) => {
        logError('processUser', err);
        setTimeout(() => {
         processUser(config, tableService, user, iteration);
        }, 5000);
      });
    }
  });
}

function processPartition(config, tableService, partition, iteration, cb) {

  let stats = {
    users : 0,
    absoluteLocations : 0,
    inferredLocations : 0,
    finalLocations : 0
  };

  let query = new azure.TableQuery().where('PartitionKey == ?', partition.toString());

  function processBatch(tableService, entries) {
    let all = [];
    for (let entry of entries) {
      let user = detablify(entry);
      all.push(processUser(config, tableService, user, iteration, stats));
    }
    return Promise.all(all);
  }

  // Fetch the next batch of entries
  function nextBatch(contToken) {

    tableService.queryEntities(config.get('user_table'), query, contToken, (err, result) => {

      if (err) {
        logError('processPartition', err);
        setTimeout(() => {
          nextBatch(contToken);
        }, 5000);
        return;
      }

      processBatch(tableService, result.entries)
      .then(() => {
        if (result.continuationToken) {
          // If there are more entries, go round again
          process.nextTick(() => {
            nextBatch(result.continuationToken);
          });
        }
        else {
          // Finished processing a partition
          cb(stats);
        }
      })
      .catch((err) => {
        logError('processBatch', err);
      });
    });
  }

  nextBatch(null);
}

function updateStatus(config, tableService, partition, iteration, status, cb) {
  var row = {
    PartitionKey : 'status',
    RowKey : partition.toString(),
    iteration : iteration.toString(),
    status : status
  };
  tableService.insertOrReplaceEntity(
    config.get('command_table'), tablify(row), (err, result) => {
    if (err) {
      debug(err);
    }
    cb(null, result);
  });
}

function pumpCommandQueue(config, tableService, queueService, retry) {

  let commandQueue = config.get('command_queue');

  let options = { visibilityTimeout : 60 };
  queueService.getMessages(commandQueue, options, (err, result) => {

    debug(err);

    if (result.length > 0) { 

      timerStart('processPartition');

      let msg = result[0];
      var message = JSON.parse(msg.messageText);
      debug('\npartition: ' + message.partition + ' iteration: ' + message.iteration);
      processPartition(config, tableService, message.partition, message.iteration, (stats) => {

        timerEnd('processPartition');
        debug(JSON.stringify(stats));

        updateStatus(
          config, tableService, message.partition, message.iteration, 'processed', (err) => {
            debug(err);
            queueService.deleteMessage(commandQueue, msg.messageId, msg.popReceipt, (err) => {
              debug(err);
              process.nextTick(() => {
                pumpCommandQueue(config, tableService, queueService, 3);
              });
            });
          }
        );
      });
    }
    else {
      if (retry > 0) {
        let timeout = _debug ? 3000 : 30000;
        console.log('Nothing in the command queue, sleeping');
        setTimeout(() => {
          pumpCommandQueue(config, tableService, queueService, --retry);
        }, timeout);
      }
      else {
        console.log('Nothing in the command queue, exiting');
      }
    }
  });
}


function main() {

  nconf.env().argv().defaults({config:'localConfig.json'});

  let configFile = nconf.get('config');
  let config = nconf.file({file:configFile, search:true});

  nconf.defaults({debug:_debug});
  _debug = config.get('debug');
  debug('debug mode');

  nconf.required(['table_storage_account', 'table_storage_key']);
  let tableService = azure.createTableService(
    config.get('table_storage_account'),
    config.get('table_storage_key')
  );

  let queueService = azure.createQueueService(
    config.get('table_storage_account'),
    config.get('table_storage_key')
  );

  pumpCommandQueue(config, tableService, queueService, 3);
}

if (require.main === module) {
    main();
}
