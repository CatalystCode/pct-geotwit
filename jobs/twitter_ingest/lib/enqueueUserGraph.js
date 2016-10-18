'use strict';

var EnqueueStage = require('./enqueue.js');

class EnqueueUserGraph extends EnqueueStage {
  constructor(config) {
    super(config);
    this.tableName = config.get('usergraph_queue');
  }
}


