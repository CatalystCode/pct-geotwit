'use strict';

var azure = require('azure-storage');
var PipeStage = require('./pipe_stage.js');

module.exports = class Enqueue extends PipeStage {

  constructor(config) {
    super(config);

    var queueService = azure.createQueueService(
      config.get('queue_storage_account'),
      config.get('queue_storage_key')
    );

    // Child class expected to provide this
    this.queueName = null;
  }

  init() {
    return Promise((resolve, reject) => {
      queueService.createQueueIfNotExists(this.queueName, (err, result) => {
        if (err) {
          console.warn('create user graph queue');
          console.warn(err.stack);
          reject(err);
        }
        resolve(null);
      });
    });
  }

  process(tweet, cb) {
    let msg = JSON.stringify(tweet);
    queueService.createMessage(this.queueName, msg, (err, result) => {
      if (err) {
        console.warn('queueing tweet to: ' + this.queueName);
        console.warn(err.stack);
        cb(null);
      }
      cb(tweet);
    });
  }
}


