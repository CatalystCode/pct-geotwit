'use strict';

var EnqueueStage = require('./enqueue.js');

class EnqueueUserGraph extends EnqueueStage {
  constructor(config) {
    super(config);
    this.queueName = config.get('usergraph_queue');
  }

}

module.exports = EnqueueUserGraph;
