'use strict';

var azure = require('azure-storage');
var PipeStage = require('./pipe_stage.js');

class Enqueue extends PipeStage {

  constructor(config) {
    super(config);

    this.queueService = azure.createQueueService(
      config.get('table_storage_account'),
      config.get('table_storage_key')
    );
    this.queueService.messageEncoder = null

    // Child class expected to provide this
    this.queueName = null;
  }

  init() {
    return new Promise((resolve, reject) => {
      this.queueService.createQueueIfNotExists(this.queueName, (err, result) => {
        if (err) {
          console.warn('create user graph queue');
          console.warn(err.stack);
          reject(err);
        }
        resolve(null);
      });
    });
  }

  process(tweet) {
    return new Promise((resolve, reject) => {
      let msg = JSON.stringify(tweet);
      this.queueService.createMessage(this.queueName, msg, (err, result) => {
        if (err) {
          reject(err);
        }
        else {
          resolve(tweet);
        }
      });
    });
  }
}

module.exports = Enqueue;
