var azure = require("azure");
var nconf = require("nconf");
var geolib = require("geolib");

var QUEUE = "tweetq";
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
      add(row, k, user[k]);
    }
  }

  tableService.insertOrMergeEntity(USER_TABLE, row, (err, result) => {
    if (cb) {
      cb(err, result);
    }
  });
}

function updateRepliedBy(tableService, userId, repliedBy) {

  /*
   * Add repliedBy to userId's list of users that have ever replied to
   * them. repliedBy and repliedTo form the directed edges of the graph that
   * we'll ultimately use to infer location
   */

  var partKey = userId.slice(0, 2);

  var user = {
    PartitionKey : partKey,
    RowKey : userId
  };

  var tableQuery = new azure.TableQuery().select("replied_by").where("PartitionKey == ?", partKey)
  .and("RowKey == ?", userId);

  tableService.queryEntities(USER_TABLE, tableQuery, null, (err, result) => {

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
        var entry = result.entries[0];
        user.replied_by = JSON.parse(entry.replied_by._);
      }
    }

    if (!(repliedBy in user.replied_by)) {
      user.replied_by.push(repliedBy);
      writeUser(tableService, user);
    }
  });
}

function condenseLocations(user) {

  var geos = {};
  var places = {};

  for (var location of user.locations) {
    if ("place" in location) {

      var place = location.place;
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

      var geo = location.geo;
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
  for (var place in places) {
    user.locations.push({ "place" : places[place] });
  }

  for (var geo in geos) {
    user.locations.push({ "geo" : geos[geo] });
  }

  return user;
}


function recalcLocation(user) {

  var latlons = [];

  for (var location of user.locations) {

    if ("place" in location) {

      // Place is a bounding box, put the user in the centre of that

      var place = location.place;
      if (place.bounding_box.type == "Polygon") {
        var coords = place.bounding_box.coordinates[0];
        var bbox = [
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
      var geo = location.geo;
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
  // for frequency but the statistical method for doing that (l1 mulivariate median)
  // is not easily available in Node (yet)

  if (latlons.length > 0) {
    var loc = geolib.getCenter(latlons);
    user.location = {
      latitude : loc.latitude,
      longitude : loc.longitude,
      confidence : 1
    };
  }

  return user;
}

function processMessage(tableService, queueService, msg, cb) {

  /* Process an individual tweet. We're interested in tweets that are geotagged
   * or are in reply to to someone. geotagged tweets end up being the fixed
   * nodes in a graph the edges of which are built by tracking who's replied
   * to who
   */

  // Arbitrarily partition user table on first two digits of user id
  var userId = msg.user_id.toString();
  var partKey = userId.slice(0, 2);
  console.log(msg);

  var tableQuery = new azure.TableQuery().where("PartitionKey == ?", partKey)
  .and("RowKey == ?", userId);

  tableService.queryEntities(USER_TABLE, tableQuery, null, (err, result) => {

    if (err) {
      console.warn(err);
      return;
    }

    var update = false;
    var locationUpdate = false;

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
    }

    if ("in_reply_to" in msg && msg.in_reply_to != null) {
      if (!(msg.in_reply_to in user.replied_to)) {
        // If tweet replies to another user, add the edge from them to us
        update = true;
        repliedTo = msg.in_reply_to.toString();
        user.replied_to.push(repliedTo);
        updateRepliedBy(tableService, repliedTo, userId);
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
      console.warn("queue.getMessages");
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
