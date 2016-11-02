'use strict';

var fs = require('fs');
var nconf = require('nconf');
var twitter = require('twitter');

let https = require('https');
https.globalAgent = new https.Agent({keepAlive:true, maxSockets:8, maxFreeSockets:8});

function processTweet(tweet, pipeline) {

  if (tweet.limit) {
    // We're being told we're matching more than our limit allows
    // Nothing to be done unless we want to partner with Twitter
    // console.warn(tweet);
    return;
  }

  let first = Promise.resolve(tweet);
  return pipeline.reduce(function(result, cls) {
    return result = result.then(cls.process.bind(cls));
  }, first);
}

function filter(config, cb) {

  config.required(['tweet_filter']);

  let filter = config.get('tweet_filter');
  if (typeof(filter) === 'string') {
    filter = JSON.parse(filter);
  }

  var client = new twitter({
    consumer_key: config.get('twitter_consumer_key'),
    consumer_secret: config.get('twitter_secret'),
    access_token_key: config.get('twitter_access_token'),
    access_token_secret: config.get('twitter_access_token_secret')
  });

  client.stream(
    '/statuses/filter', filter,
    function(stream) {
      stream.on('error', function(err) {
        console.error('stream error');
        console.error(err);
        cb('error', null);
      });
      stream.on('data', function(tweets) {
        cb(null, tweets);
      });
      stream.on('end', (err) => {
        console.error('twitter stream ended');
        console.error(err);
        cb('end', null);
      });
      stream.on('destroy', (err) => {
        console.error('twitter stream destroyed');
        cb(err, null);
      });
    }
  );
}

function main() {

  nconf.env().argv().defaults({config:'localConfig.json'});

  let configFile = nconf.get('config');
  let config = nconf.file({file:configFile, search:true});

  let pipeline = [];
  let pipelineSpec = config.get('pipeline');
  for (let stage of pipelineSpec.split(",")) {
    console.log('adding stage:' + stage);
    let cls = require('./lib/' + stage);
    pipeline.push(new cls(config));
  }

  let inits = [];
  for (let stage of pipeline) {
    inits.push(stage.init());
  }

  process.on('exit', () => {
    console.log('exiting');
  });

  function filterCallback(err, tweet) {
    if (err) {
      console.warn('Error from streaming api...');
      if (err === 'end') {
        console.log('Retrying...');
        setTimeout(() => {
          filter(config, filterCallback); 
        }, 5000);
      }
    }
    else {
      processTweet(tweet, pipeline);
    }
  }

  Promise.all(inits)
  .then(() => {
    console.log('Starting...');
    filter(config, filterCallback);
  });
}

if (require.main === module) {
  main();
}
