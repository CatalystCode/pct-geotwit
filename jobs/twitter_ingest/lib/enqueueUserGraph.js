'use strict';

var EnqueueStage = require('./enqueue.js');

class EnqueueUserGraph extends EnqueueStage {
  constructor(config) {
    super(config);
    this.queueName = config.get('usergraph_queue');
  }

  process(tweet) {
    return new Promise((resolve, reject) => {
      let row = {
        'user_id' : tweet.user.id,
        'user_screen_name' : tweet.user.screen_name,
        'timestamp' : tweet.timestamp_ms,
        'lang' : tweet.lang,
        'in_reply_to' : tweet.in_reply_to_user_id,
        'place' : JSON.stringify(tweet.place),
        'geo' : JSON.stringify(tweet.geo)
      };
      super.process(row);
    });
  }
}

module.exports = EnqueueUserGraph;
