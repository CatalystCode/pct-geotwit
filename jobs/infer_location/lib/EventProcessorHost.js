var path = require("path");
var guid = require("guid");
var azure = require("azure");
var EventHubClient = require("azure-event-hubs").Client;

function EventProcessorHost(blobService, eventHubConnectionString)
{
  this.blobService = blobService;

  this.id = guid.raw();

  this.container = "eph";
  this.leaseDuration = 30;
  this.leaseDelay = 10;
  this.checkpointInterval = 10;
  //this.leaseDuration = 6000;
  //this.leaseDelay = 10000;
  this.hubConnectionString = eventHubConnectionString;

  var stringParts = eventHubConnectionString.split("=");
  this.hubName = stringParts[stringParts.length - 1];
  console.log(this.hubName);

  this.partitions = [];
  this.currentPartitions = {};

  this.evp = null;
  this.consumerGroup = null;

  this.eventHubClient = new EventHubClient.fromConnectionString(eventHubConnectionString);
}

EventProcessorHost.prototype.init = function() {
  return this.eventHubClient.getPartitionIds()
  .then((partitions) => {
    this.partitions = partitions;
    return this._initPartitionBlobs();
  });
}

EventProcessorHost.prototype.registerEventProcessor = function(consumerGroup, evp) {
  this.evp = evp;
  this.consumerGroup = consumerGroup;

  this.timer = setInterval(() => {
    this._tick();
  }, 1000);

  this.workerCheckpointTimer = setInterval(() => {
    this.checkpointWorker();
  });
}

EventProcessorHost.prototype.checkpointWorker = function() {
  var blob = path.join(this.hubName, this.id);
  this.blobService.createBlockBlobFromText(this.container, blob, this.id, (err, result) => {
  });
}

EventProcessorHost.prototype.checkpointPartition = function(partition) {
  var blob = path.join(this.hubName, partition);
  var content = JSON.stringify({checkpoint:"soemthing"});
  this.blobService.createBlockBlobFromText(this.container, blob, content, (err, result) => {
  });
}

EventProcessorHost.prototype._tick = function() {

  if (Object.keys(this.currentPartitions).length < this.partitions.length) {
    this._acquirePartition()
    .then((partition) => {
      console.log(this.id + ":acquired " + partition);
      if (partition) {
        setInterval(() => {
          this.checkpointPartition(partition);
        }, this.checkpointInterval * 1000);

        /*this.eventHubClient.createReceiver(this.consumerGroup, partition)
        .then((rx) => {
          rx.on("errorReceived", (err) => {
            console.warn("error:" + err);
          });
          rx.on("message", (msg) => {
            console.log(msg);
          });
        });*/
      }
    });
  }
}

EventProcessorHost.prototype._acquirePartition = function() {

  var self = this;

  function tryPartition(partition, cb) {
    var blob = path.join(self.hubName, partition);
    var options = { leaseDuration: self.leaseDuration };
    self.blobService.acquireLease(self.container, blob, options, (err, result) => {
      if (err) {
        cb(err, null);
      }
      else {
        self.currentPartitions[partition] = result.id;

        setTimeout(() => {
          self._renewLease(partition);
        }, (self.leaseDuration / 2) * 1000);
        cb(null, partition);
      }
    });
  }

  return new Promise((resolve, reject) => {

    // Try each partition in turn..
    var partitions = this.partitions.slice().filter((x) => {
      return (!(x in this.currentPartitions));
    });

    function onPartitionCallback(err, result) {
      if (err) {
        if (partitions.length > 1) {
          partitions.shift();
          tryPartition(partitions[0], onPartitionCallback);
        }
        else {
          resolve(null);
        }
      }
      else {
        resolve(result);
      }
    }

    tryPartition(partitions[0], onPartitionCallback);
  });
}

EventProcessorHost.prototype._renewLease = function(partition) {
  var blob = path.join(this.hubName, partition);
  var leaseId = this.currentPartitions[partition];
  this.blobService.renewLease(this.container, blob, leaseId, (err, result) => {
    if (err) {
      console.log("renew error: " + err);
      setTimeout(() => {
        delete this.currentPartitions[partition];
      }, this.leaseDelay * 1000);
    }
    else {
      console.log("renew result: " + result);

      setTimeout(() => {
        this._renewLease(partition);
      }, (this.leaseDuration / 2) * 1000);
    }
  });
}


EventProcessorHost.prototype._breakLease = function(partition) {
  var blob = path.join(this.hubName, partition);
}

EventProcessorHost.prototype._releasePartition = function(partition) {

  return new Promise((resolve, reject) => {
    var blob = path.join(self.hubName, partition);
    var leaseId = this.currentPartitions[partition];
    this.blobService.releaseLease(this.container, blob, lease, (err, result) => {
      if (err) reject(err);
      else {
        resolve(result);
        setTimeout(() => {
          delete currentPartitions[partition];
        }, this.leaseDelay);
      }
    });
  });
}

EventProcessorHost.prototype._initPartitionBlobs = function() {

  /* Make sure we have exactly the blob files we need to coordinate
   * multiple jobs across the partitions. If we've got what we need then
   * we're in the middle of a run. If we don't then create fresh ones
   */

  var containerLeaseId = null;
  return new Promise((resolve, reject) => {
    // Create the container (doesn't matter if it already exists)
    this.blobService.createContainer(this.container, (err, result) => {
      resolve(err != null);
    });
  })
  .then(() => {
    // Mutex the container whilst we create our blobs
    return new Promise((resolve, reject) => {
      var options = { leaseDuration: this.leaseDuration };
      this.blobService.acquireLease(this.container, null, options, (err, result) => {
        if (err) reject(err);
        else {
          resolve(result);
        }
      });
    });
  })
  .then((result) => {

    // List contents of container
    containerLeaseId = result.id;
    return new Promise((resolve, reject) => {
      this.blobService.listBlobsSegmentedWithPrefix(
        this.container, this.hubName, null, (err, result) => {
        if (err) reject(err);
        else {
          resolve(result.entries.map((x) => { return x.name; }));
        }
      });
    });
    return [];
  })
  .then((existingBlobs) => {

    // Figure out if we need to recreate blobs
    var recreateAll = false;
    var requiredBlobs = this.partitions.map((x) => { return path.join(this.hubName, x); });
    for (var blob in requiredBlobs) {
      if (!(blob in existingBlobs)) {
        recreateAll = true;
      }
    }

    var all = [];
    if (recreateAll) {
      var content = JSON.stringify({checkpoint:null});
      for (var partition in this.partitions) {
        var blob = path.join(this.hubName, partition);
        all.push(new Promise((resolve, reject) => {
          this.blobService.createBlockBlobFromText(this.container, blob, content, (err, result) => {
            if (err) reject(err);
            else {
              resolve(result);
            }
          });
        }));
      };
    }
    return all.length > 0 ? Promise.all(all) : [];
  })
  .then(() => {
    return new Promise((resolve, reject) => {
      this.blobService.releaseLease(this.container, null, containerLeaseId, (err, result) => {
        console.log(this.id + ":" + " container released");
        console.log(result);
        if (err) reject(err);
        else resolve(result);
      });
    });
  });
}

module.exports = EventProcessorHost;
