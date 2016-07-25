var async = require('async');
var azure = require('azure-storage');
var twitter = require('twitter');
var nconf = require('nconf');
var moment = require('moment');

nconf.file({ file: 'localConfig.json', search: true }).env();

console.log(nconf.get("STORAGE_ACCOUNT"));
console.log(nconf.get("STORAGE_KEY"));

var USERGRAPH_QUEUE_NAME = nconf.get("TWEET_USERGRAPH_QUEUE_NAME");
console.log(USERGRAPH_QUEUE_NAME);
var PIPELINE_QUEUE_NAME = nconf.get("TWEET_PIPELINE_QUEUE_NAME");
console.log(PIPELINE_QUEUE_NAME);
var TABLE_NAME = nconf.get("TWEET_TABLE_NAME");
console.log(TABLE_NAME);

var tableService = azure.createTableService(
  nconf.get("STORAGE_ACCOUNT"),
  nconf.get("STORAGE_KEY")
);

var queueService = azure.createQueueService(
  nconf.get("STORAGE_ACCOUNT"),
  nconf.get("STORAGE_KEY")
);

function filter(filters, cb) {

  var client = new twitter({
  consumer_key: nconf.get("TWITTER_CONSUMER_KEY"),
  consumer_secret: nconf.get("TWITTER_CONSUMER_SECRET"),
  access_token_key: nconf.get("TWITTER_ACCESS_TOKEN_KEY"),
  access_token_secret: nconf.get("TWITTER_ACCESS_TOKEN_SECRET")
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

  tableService.insertEntity(TABLE_NAME, row, (err, result, response) => {
    if (err) {
      console.warn("inserting tweet");
      console.warn(response);
      console.warn(err.stack);
    }
  });

  var msg = JSON.stringify(detablify(row));
  // add the message for user graph processing
  queueService.createMessage(USERGRAPH_QUEUE_NAME, msg, (err, result) => {
    if (err) {
      console.warn("queueing tweet");
      console.warn(err.stack);
    }
  });
  ingestTweet(tweet);
}

function ingestTweet(tweet){
    var iso_8601_created_at = moment(tweet.created_at, 'dd MMM DD HH:mm:ss ZZ YYYY', 'en');
    var tweetEssentials = {
        // we use moment.js to pars the "strange"" Twitter dateTime format
        created_at:  iso_8601_created_at,
        id: tweet.id,
        geo: tweet.geo,
        lang: tweet.lang,
        source: tweet.source,
        text: tweet.text,
        user_id: tweet.user.id,
        user_followers_count: tweet.user.followers_count,
        user_friends_count: tweet.user.friends_count,
        user_name: tweet.user.name,
    };
    var message = {
        source: 'twitter',
        created_at: iso_8601_created_at,
        message: tweetEssentials
    }

    queueService.createMessage(PIPELINE_QUEUE_NAME, JSON.stringify(message), function(err, result, response) {
        if (err) {
            console.log('error: ' + err);
        }
        console.log('success');
    });
}
function getKeywordList() {

  var refDataContainer = nconf.get("REFERENCE_DATA_BLOB_CONTAINER");
  if (!refDataContainer) {
    return null;
  }

  var blobService = azure.createBlobService(
    nconf.get("STORAGE_ACCOUNT"),
    nconf.get("STORAGE_KEY")
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
    nconf.get("STORAGE_ACCOUNT"),
    nconf.get("STORAGE_KEY")
  );

  var queueService = azure.createQueueService(
    nconf.get("STORAGE_ACCOUNT"),
    nconf.get("STORAGE_KEY")
  );

  tableService.createTableIfNotExists(TABLE_NAME, (err, result) => {
    if (err) {
      console.warn("createTable");
      console.warn(err.stack);
      process.exit(1);
    }

    queueService.createQueueIfNotExists(USERGRAPH_QUEUE_NAME, (err, result) => {
      if (err) {
        console.warn("create user graph queue");
        console.warn(err.stack);
        process.exit(1);
      }
      queueService.createQueueIfNotExists(PIPELINE_QUEUE_NAME, (err, result) => {
          if (err) {
            console.warn("create user graph queue");
            console.warn(err.stack);
            process.exit(1);
          }
          var filterSpec = {};

          var bbox = nconf.get("BOUNDING_BOX");
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
        })
    })
  });
}

if (require.main === module) {
    main();
}
