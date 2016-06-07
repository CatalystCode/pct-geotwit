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

function main() {
  count = 0
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
    count += 1
    console.log(tweet);
    console.log(count);
  });
}

if (require.main === module) {
    main();
}
