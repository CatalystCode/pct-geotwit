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

  console.log(row);
  tableService.insertOrReplaceEntity(TABLE, row, (err, result) => {
    if (err) {
      console.warn("insert: " + err);
      console.log(row);
    }
    updated++;
  });
}

function foreach(tableService, user) {

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
}

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
        console.warn(err);
        process.exit(1);
      }
      processBatch(result.entries);
      if (result.continuationToken) {
        process.nextTick(() => {
          nextBatch(result.continuationToken);
          console.log("Processed: " + processed);
          console.log("Updated: " + updated);
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
