'use strict';

module.exports = class PipeStage {

  constructor(config) {
    this.config = config;
  }

  init() {
    // To be implemented by child class
  }

  process(item) {
    return item;
  }
}
