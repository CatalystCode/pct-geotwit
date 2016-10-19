'use strict';

var fs = require('fs');
var nconf = require('nconf');
var twitter = require('twitter');

function filter(config, cb) {

  config.required(['tweet_filter']);

  var client = new twitter({
    consumer_key: config.get('twitter_consumer_key'),
    consumer_secret: config.get('twitter_secret'),
    access_token_key: config.get('twitter_access_token'),
    access_token_secret: config.get('twitter_access_token_secret')
  });

  client.stream(
    '/statuses/filter', config.get('tweet_filter'),
    function(stream) {
      stream.on('error', function(err) {
        cb(err, null);
      });
      stream.on('data', function(tweets) {
        cb(null, tweets);
      });
    }
  );
}

function processTweet(tweet, pipeline) {

  if (tweet.limit) {
    // We're being told we're matching more than our limit allows
    // Nothing to be done unless we want to partner with Twitter
    console.warn(tweet);
    return;
  }

  var head = null;
  for (let stage of pipeline) {
    if (!head) {
      head = stage.process(tweet);
    }
    else {
      head.then(stage.process(tweet));
    }
  }

  if (head != null) {
    head.catch((e) => {
      console.warn(e.stack);
    });
  }
}

function main() {

  nconf.env().argv().defaults({config:'localConfig.json'});

  let configFile = nconf.get('config');
  let config = nconf.file({file:configFile, search:true});

  let pipeline = [];
  let pipelineSpec = config.get('pipeline');
  for (let stage of pipelineSpec) {
    let cls = require('./lib/' + stage);
    pipeline.push(new cls(config));
  }

  let promises = [];
  for (let stage of pipeline) {
    promises.push(stage.init());
  }

  Promise.all(promises)
  .then((values) => {
    filter(
      config,
      (err, tweet) => {
        if (err) {
          console.warn(err.stack);
          process.exit(1);
        }
        processTweet(tweet, pipeline);
      }
    );
  });
}

if (require.main === module) {
  main();
}
