'use strict';

var azure = require('azure-storage');
var PipeStage = require('./pipe_stage.js');

class Dedupe extends PipeStage {

  constructor(config) {
    super(config);

    this.tableService = azure.createTableService(
      config.get('table_storage_account'),
      config.get('table_storage_key')
    );

    this.tableName = config.get('tweet_table');
  }

  init() {
    return new Promise((resolve, reject) => {
      this.tableService.createTableIfNotExists(TABLE_NAME, (err, result) => {
        if (err) {
          console.warn('createTable');
          console.warn(err.stack);
          reject(err);
        }
        resolve(null);
      });
    });
  }

  _tablify(o) {
    // Given an object, return a Table row
    var t = {};
    for (var k in o) {
      t[k] = { _ : t[k] }
    }

    return t
  }

  _detablify(t) {
    // Given a Table row, return as an object
    var o = {};
    for (var k in t) {
      if (k[0] !== '.') {
        o[k] = t[k]._;
      }
    }
    return o;
  }

  process(tweet, cb) {

    return new Promise((resolve, reject) => {

      let row = {
        'PartitionKey' : Math.floor(tweet.timestamp_ms / (24 * 60 * 60 * 1000)).toString(),
        'RowKey' : tweet.id.toString(),
        'user_id' : tweet.user.id,
        'user_screen_name' : tweet.user.screen_name,
        'timestamp' : tweet.timestamp_ms,
        'text' : tweet.text,
        'lang' : tweet.lang,
        'in_reply_to' : tweet.in_reply_to_user_id,
        'place' : JSON.stringify(tweet.place),
        'geo' : JSON.stringify(tweet.geo)
      };

      tableService.insertEntity(this.tableName, _tablify(row), (err, result, response) => {
        if (err) {
          console.warn('inserting tweet');
          console.warn(response);
          console.warn(err.stack);
          reject(err);
        }

        resolve(tweet);
      });

    });
  }
}
