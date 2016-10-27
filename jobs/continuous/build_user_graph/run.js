'use strict';

let nconf = require("nconf");
let geolib = require("geolib");
let azure = require("azure-storage");

function random(low, high) {
  return Math.random() * (high - low) + low;
}

function writeUser(config, tableService, user, cb) {

  function add(r, k, v) {
    r[k] = { _ : v };
  }

  let row = {};
  for (let k in user) {
    if (typeof(user[k]) == 'object') {
      add(row, k, JSON.stringify(user[k]));
    }
    else {
      add(row, k, user[k]);
    }
  }

  tableService.insertOrMergeEntity(config.get('user_table'), row, (err, result) => {
    if (cb) {
      cb(err, result);
    }
  });
}

function updateRepliedBy(config, tableService, userId, repliedBy) {

  /*
   * Add repliedBy to userId's list of users that have ever replied to
   * them. repliedBy and repliedTo form the directed edges of the graph that
   * we'll ultimately use to infer location
   */

  let userTable = config.get('user_table');
  let partKey = userId.slice(0, 2);

  let user = {
    PartitionKey : partKey,
    RowKey : userId
  };

  let tableQuery = new azure.TableQuery().select("replied_by").where("PartitionKey == ?", partKey)
  .and("RowKey == ?", userId);

  tableService.queryEntities(userTable, tableQuery, null, (err, result) => {

    if (err) {
      console.warn("updateRepliedBy");
      console.warn(err.stack);
    }
    else {
      if (result.entries.length == 0) {
        // New user
        user.locations = [];
        user.replied_to = [];
        user.replied_by = [];
        user.location = {
          "latitude" : random(-90, 90),
          "longitude" : random(-180, 180),
          "confidence" : 0
        }; // A random location to start with
      }
      else {
        // Existing user
        let entry = result.entries[0];
        user.replied_by = JSON.parse(entry.replied_by._);
      }
    }

    if (!(repliedBy in user.replied_by)) {
      user.replied_by.push(repliedBy);
      writeUser(config, tableService, user);
    }
  });
}

function condenseLocations(user) {

  let geos = {};
  let places = {};

  for (let location of user.locations) {
    if ("place" in location) {

      let place = location.place;
      if (!("occurrences" in place)) {
        place.occurrences = 1;
      }

      if (!(place.id in places)) {
        places[place.id] = place;
      }
      else {
        places[place.id].occurences += place.occurences;
      }
    }
    else if ("geo" in location) {

      let geo = location.geo;
      if (!("occurrences" in geo)) {
        geo.occurrences = 1;
      }

      if (!(geo.coordinates in geos)) {
        geos[geo.coordinates] = geo;
      }
      else {
        geos[geo.coordinates].occurrences += geo.occurences;
      }
    }
  }

  user.locations = [];
  for (let place in places) {
    user.locations.push({ "place" : places[place] });
  }

  for (let geo in geos) {
    user.locations.push({ "geo" : geos[geo] });
  }

  return user;
}


function recalcLocation(user) {

  let latlons = [];

  for (let location of user.locations) {

    if ("place" in location) {

      // Place is a bounding box, put the user in the centre of that

      let place = location.place;
      if (place.bounding_box.type == "Polygon") {
        let coords = place.bounding_box.coordinates[0];
        let bbox = [
          { longitude:coords[0][0], latitude:coords[0][1] },
          { longitude:coords[1][0], latitude:coords[1][1] },
          { longitude:coords[2][0], latitude:coords[2][1] },
          { longitude:coords[3][0], latitude:coords[3][1] },
        ];

        latlons.push(geolib.getCenter(bbox));
      }
      else {
        console.warn("unknown bounding box type");
      }
    }
    else if ("geo" in location) {
      let geo = location.geo;
      if (geo["type"] == "Point") {
        latlons.push({latitude:geo.coordinates[0], longitude:geo.coordinates[1]});
      }
      else {
        console.warn("unknown geo type");
        console.warn(geo);
      }
    }
  }

  // Put the user at the centre of all the latlons they've ever reported.
  // Note: We could improve accuracy by rejecting outliers and/or weighting
  // for frequency but the statistical method for doing that (l1 muliletiate median)
  // is not easily available in Node (yet)

  if (latlons.length > 0) {
    let loc = geolib.getCenter(latlons);
    user.location = {
      latitude : loc.latitude,
      longitude : loc.longitude,
      confidence : 1
    };
  }

  return user;
}

function processMessage(config, tableService, queueService, msg, cb) {

  /* Process an individual tweet. We're interested in tweets that are geotagged
   * or are in reply to to someone. geotagged tweets end up being the fixed
   * nodes in a graph the edges of which are built by tracking who's replied
   * to who
   */

  // Arbitrarily partition user table on first two digits of user id

  msg = JSON.parse(msg);

  let userId = msg.user_id.toString();
  let partKey = userId.slice(0, 2);
  let userTable = config.get('user_table');

  let tableQuery = new azure.TableQuery().where("PartitionKey == ?", partKey)
  .and("RowKey == ?", userId);

  tableService.queryEntities(userTable, tableQuery, null, (err, result) => {

    if (err) {
      console.error(err);
      return;
    }

    let update = false;
    let locationUpdate = false;

    let user = {
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
      let entry = result.entries[0];
      user.locations = JSON.parse(entry.locations._);
      user.replied_to = JSON.parse(entry.replied_to._);
    }

    if ("in_reply_to" in msg && msg.in_reply_to != null) {
      if (!(msg.in_reply_to in user.replied_to)) {
        // If tweet replies to another user, add the edge from them to us
        update = true;
        let repliedTo = msg.in_reply_to.toString();
        user.replied_to.push(repliedTo);
        updateRepliedBy(config, tableService, repliedTo, userId);
      }
    }

    // Twitter has two kinds of geolocation
    // geo (deprecated but still out there) is a point
    // place is roughly town sized and comes as a bounding box

    if ("geo" in msg && msg.geo != 'null') {
      update = true;
      locationUpdate = true;
      user.locations.push({"geo":JSON.parse(msg.geo)});
    }

    if ("place" in msg && msg.place != 'null') {
      update = true;
      locationUpdate = true;
      user.locations.push({"place":JSON.parse(msg.place)});
    }

    if (update) {
      if (locationUpdate) {
        // New geo information for this user, recalc their position
        user = condenseLocations(user);
        user = recalcLocation(user);
      }
      writeUser(config, tableService, user, cb);
    }
    else {
      cb(null, true);
    }
  });
}

function onMessage(config, tableService, queueService, message) {
  return new Promise((resolve, reject) => {
    processMessage(config, tableService, queueService, message.messageText, (err, result) => {
      if (err) {
        resolve([]);
      } else {
        resolve([message.messageId, message.popReceipt]);
      }
    });
  });
}

function pump(config, tableService, queueService) {

  let options = { numOfMessages:32 };
  let userGraphQueue = config.get('usergraph_queue');

  queueService.getMessages(userGraphQueue, options, (err, result) => {
    if (!err) {
      Promise.all(result.map((msg) => onMessage(config, tableService, queueService, msg)))
      .then((results) => {
        for (let result of results) {
          if (result.length > 0) {
            queueService.deleteMessage(userGraphQueue, result[0], result[1], (err, result) => {
            });
          }
        }
        process.nextTick(() => {
          pump(config, tableService, queueService);
        });
      })
      .catch((e) => {
        console.error(e.stack);
      });
    }
    else {
      console.warn("queue.getMessages");
      console.warn(err);
    }
  });
}

function main() {

  let configFile = nconf.argv().get('config');
  nconf.defaults({config:'localConfig.json'});
  let config = nconf.file({file:configFile, search:true});

  nconf.required(['table_storage_account', 'table_storage_key']);
  let tableService = azure.createTableService(
    config.get("table_storage_account"),
    config.get("table_storage_key")
  );

  let queueService = azure.createQueueService(
    config.get("table_storage_account"),
    config.get("table_storage_key")
  );
  queueService.messageEncoder = null;

  nconf.required(['user_table']);
  tableService.createTableIfNotExists(config.get('user_table'), (err, result) => {
    if (err) {
      console.error(err);
      process.exit(1);
    }
    pump(config, tableService, queueService);
  });
}

if (require.main === module) {
    main();
}
