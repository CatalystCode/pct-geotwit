'use strict';

var EnqueueStage = require('./enqueue.js');

class EnqueueNlp extends EnqueueStage {
  constructor(config) {
    super(config);
    this.queueName = config.get('nlp_queue');
  }

  process(tweet) {
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

    super(message);
  }
}

module.exports = EnqueueNlp;
