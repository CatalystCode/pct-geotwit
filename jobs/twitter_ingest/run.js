var nconf = require("nconf")
var azure = require("azure")
var twitter = require("twitter")

var TABLE = "tweets";
var QUEUE = "tweetq";

var config = nconf.env().file({ file: '../../localConfig.json' });

function filter(filters, cb) {

  var client = new twitter({
    consumer_key: config.get("consumer_key"),
    consumer_secret: config.get("consumer_key_secret"),
    access_token_key: config.get("access_token"),
    access_token_secret: config.get("access_token_secret")
  });

  client.stream(
    "/statuses/filter", filters,
    function(stream) {
      stream.on("error", function(err) {
        cb(err, null);
      });
      stream.on("data", function(tweets) {
        cb(null, tweets);
      });
    }
  );
}

function processTweet(tweet, tableService, queueService) {

  function add(r, k, v) {
    r[k] = { _ : v };
  }

  function detablify(t) {
    var o = {};
    for (var k in t) {
      if (k[0] != '.') {
        o[k] = t[k]._;
      }
    }
    return o;
  }


  /* Hmm.. batching often errors with 'one of the inputs is invalid'
     that we don't see when we just hammer the Table.

  function sendBatch(batch, retries) {
    tableService.executeBatch("tweets", batch, function(error, result) {
      if (error) {
        if (retries > 0) {
          console.log("retrying : " + error);
          setTimeout(() => { sendBatch(batch, --retries); }, 10000);
        } else {
          console.log("inserting tweet: " + error);
        }
      }
    });
  }*/

  if (tweet.limit) {
    // We're being told we're matching more than our limit allows
    // Nothing to be done unless we want to partner with Twitter
    console.log(tweet);
    return;
  }

  var row = {};
  add(row, "PartitionKey", Math.floor(tweet.timestamp_ms / (24 * 60 * 60 * 1000)).toString());
  add(row, "RowKey", tweet.id.toString());
  add(row, "user_id", tweet.user.id);
  add(row, "user_screen_name", tweet.user.screen_name);
  add(row, "timestamp", tweet.timestamp_ms);
  add(row, "text", tweet.text);
  add(row, "lang", tweet.lang);
  add(row, "in_reply_to", tweet.in_reply_to_user_id);
  add(row, "place", JSON.stringify(tweet.place));
  add(row, "geo", JSON.stringify(tweet.geo));

  tableService.insertEntity(TABLE, row, (err, result, response) => {
    if (err) {
      console.warn("inserting tweet");
      console.warn(err.stack);
    }
  });

  var msg = JSON.stringify(detablify(row));
  queueService.createMessage(QUEUE, msg, (err, result) => {
    if (err) {
      console.warn("queueing tweet");
      console.warn(err.stack);
    }
  });
}

function getKeywordList() {

  var refDataContainer = config.get("REFERENCE_DATA_CONTAINER_NAME");
  if (!refDataContainer) {
    return null;
  }

  var blobService = azure.createBlobService(
    config.get("AZURE_STORAGE_ACCOUNT"),
    config.get("AZURE_STORAGE_ACCESS_KEY")
  );

  var promise = new Promise((resolve, reject) => {
    blobService.listBlobsSegmentedWithPrefix(refDataContainer, "keywords", null, (err, result) => {
      if (err) {
        console.warn("blob: " + err);
        reject(err);
      }
      resolve(result.entries);
    });
  }).then((files) => {

    var all = [];
    for (var file of files) {
      all.push(new Promise((resolve, reject) => {
        blobService.getBlobToText(refDataContainer, file.name, (err, result) => {
          if (err) {
          }
          resolve("" + result);
        });
      }));
    }
    return Promise.all(all);

  }).then((results) => {
    return results.map((x) => { return x.trim(); }).join(",").split(",");
  })
  .catch((err) => {
    console.warn(err.stack);
  });

  return promise;
}

function main() {

  var tableService = azure.createTableService(
    config.get("AZURE_STORAGE_ACCOUNT"),
    config.get("AZURE_STORAGE_ACCESS_KEY")
  );

  var queueService = azure.createQueueService(
    config.get("AZURE_STORAGE_ACCOUNT"),
    config.get("AZURE_STORAGE_ACCESS_KEY")
  );

  tableService.createTableIfNotExists(TABLE, (err, result) => {
    if (err) {
      console.warn("createTable");
      console.warn(err.stack);
      process.exit(1);
    }

    queueService.createQueueIfNotExists(QUEUE, (err, result) => {
      if (err) {
        console.warn("createTable");
        console.warn(err.stack);
        process.exit(1);
      }

      var filterSpec = {};

      var bbox = config.get("twitter_ingest_bbox");
      if (bbox) {
        filterSpec.locations = bbox;
      }

      getKeywordList().then((keywords) => {

        if (keywords.length > 400) {
          console.warn("filter: >400 keywords, truncating");
          keywords = keywords.slice(0, 399);
        }

        if (keywords) {
          filterSpec.track = keywords.join(",");
        }

        filterSpec.language = "in";

        console.log("== Filter Spec ==");
        console.log(filterSpec);

        filter(
          filterSpec,
          (err, tweet) => {
            if (err) {
              console.warn("twitter");
              console.warn(err.stack);
              process.exit(1);
            }
            processTweet(tweet, tableService, queueService);
          }
        );

      })
      .catch((err) => {
        console.log(err.stack);
      });
    });
  });
}

if (require.main === module) {
    main();
}
