'use strict';

var azure = require('azure-storage');
var PipeStage = require('./pipe_stage.js');

class Dedupe extends PipeStage {

  constructor(config) {
    super(config);
  }

  init() {
    this.dupes = new Set();
    setInterval(() => {
      this.dupes = new Set();
    }, 360000);
  }

  process(tweet) {
    return new Promise((resolve, reject) => {
      resolve(tweet);
      let id = tweet.id;
      if (this.dupes.has(id)) {
        reject(id);
      }
      else {
        this.dupes.add(id);
        resolve(tweet)
      }
    });
  }
}

module.exports = Dedupe;
