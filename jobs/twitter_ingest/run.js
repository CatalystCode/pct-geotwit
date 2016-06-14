var nconf = require("nconf")
var azure = require("azure")
var twitter = require("twitter")

var config = nconf.env().file({ file: './localConfig.json' });

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

function process_tweet(tweet, tableService) {

  function add(r, k, v) {
    r[k] = { _ : v };
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

  tableService.insertOrReplaceEntity("filteredtweets", row, (err, result) => {
    if (err) {
      console.log("inserting tweet: " + err);
    }
  });
}

function init(cb) {

  var tableService = azure.createTableService(
    config.get("AZURE_STORAGE_ACCOUNT"),
    config.get("AZURE_STORAGE_ACCESS_KEY")
  );

  tableService.createTableIfNotExists("filteredtweets", function(err, result) {
    cb(err, tableService);
  });
}

function main() {
  init((err, tableService) => {
    if (err) {
      console.log(err);
      process.exit(1);
    }
    bbox_indonesia = "94.7717056274,-11.2085676193,141.0194549561,6.2744498253"
    filter({
      track: "aku,tidak,yang,kau,ini,itu,di,dan,akan,apa",
      locations: bbox_indonesia
    },
    function(err, tweet) {
      if (err) {
        console.warn(err);
        process.exit(1);
      }
      process_tweet(tweet, tableService);
    });
  });
}

if (require.main === module) {
    main();
}
