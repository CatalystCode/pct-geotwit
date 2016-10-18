'use strict';

var fs = require('fs');
var nconf = require('nconf');
var twitter = require('twitter');

function filter(config, cb) {

  config.required(['tweet_filter']);
  console.log(config.get());

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

  console.log(tweet);

  /* Hmm.. batching often errors with 'one of the inputs is invalid'
     that we don't see when we just hammer the Table.

  function sendBatch(batch, retries) {
    tableService.executeBatch('tweets', batch, function(error, result) {
      if (error) {
        if (retries > 0) {
          console.log('retrying : ' + error);
          setTimeout(() => { sendBatch(batch, --retries); }, 10000);
        } else {
          console.log('inserting tweet: ' + error);
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

  //ingestTweet(tweet);
}

function ingestTweet(tweet){
  // we use moment.js to parse the 'strange'' Twitter dateTime format
  var iso_8601_created_at = moment(tweet.created_at, 'dd MMM DD HH:mm:ss ZZ YYYY', 'en');

  var tweetEssentials = {
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

  queueService.createMessage(
    PIPELINE_QUEUE_NAME, 
    JSON.stringify(message), 
    function(err, result, response) {
      if (err) {
        console.log('error: ' + err);
      }
      console.log('success');
    }
  );
}

function main() {

  nconf.defaults({config:'localConfig.json'});

  console.log(nconf.argv().get('config'));
  let configFile = nconf.argv().get('config');
  let config = nconf.file({file:configFile, search:true});

  let pipeline = [];
  let pipelineSpec = config.get('pipeline');
  for (let stage of pipelineSpec) {
    pipeline.push(require('./lib/' + stage));
  }
  console.log(pipelineSpec);

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
}

if (require.main === module) {
  main();
}
