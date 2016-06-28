var path = require("path");
var guid = require("guid");
var azure = require("azure");
var EventHubClient = require("azure-event-hubs").Client;

function shuffle(array) {
  // Randomise an array..
  var currentIndex = array.length, temporaryValue, randomIndex;
  // While there remain elements to shuffle...
  while (0 !== currentIndex) {
    // Pick a remaining element...
    randomIndex = Math.floor(Math.random() * currentIndex);
    currentIndex -= 1;
    // And swap it with the current element.
    temporaryValue = array[currentIndex];
    array[currentIndex] = array[randomIndex];
    array[randomIndex] = temporaryValue;
  }
  return array;
}

function EventProcessorHost(blobService, eventHubConnectionString)
{
  this.blobService = blobService;

  // Our unique id
  this.id = guid.raw();

  // Container into which we'll write our state
  this.container = "eph";

  // The amount of time we'll lock a parition
  this.leaseDuration = 30;

  this.countInterval = 10;
  this.checkpointInterval = 10;
  this.workerActiveTimeout = this.checkpointInterval * 3;

  this.hubConnectionString = eventHubConnectionString;

  var stringParts = eventHubConnectionString.split("=");
  this.hubName = stringParts[stringParts.length - 1];

  console.log("Initialising..");
  console.log(this.hubName);
  console.log(this.id);

  this.partitions = [];
  this.currentPartitions = {};
  this.lastBreakTime = null;

  this.activeWorkers = [];

  this.evp = null;
  this.consumerGroup = null;
  this.latestOffsets = {};

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

  // Register a callback that will recieve messages from the hub. Messages may be delivered
  // from any partition at any time. Also.. start everything

  this.evp = evp;
  this.consumerGroup = consumerGroup;

  // Perform initial worker checkpoint then
  // set up execution timers..
  this.checkpointWorker((err, result) => {

    console.log("Initial worker checkpoint");
    this.workerCheckpointTimer = setInterval(() => {
      this.checkpointWorker();
    },  this.checkpointInterval * 1000);

    this._countWorkers();
    this.countTimer = setInterval(() => {
      this._countWorkers();
    }, this.countInterval * 1000);

    this.timer = setInterval(() => {
      this._tick();
    }, 1000);
  });
}

EventProcessorHost.prototype._calcPartitionShare = function() {

  // Figure out how many partitions this worker should have
  // if (workers % partitions != 0) the remainder will be distributed
  // amongst the lowest sorting workers

  var ourShare = (this.partitions.length / this.activeWorkers.length);
  var remainder = (this.partitions.length % this.activeWorkers.length);

  this.activeWorkers.sort();

  // These workers get one more partition than the rest
  var extraWork = this.activeWorkers.slice(0, remainder);

  // And if we're in that set, we get an extra partition
  if (this.id in extraWork) {
    return ourShare + 1;
  }

  // .. else we don't
  return ourShare;
}

EventProcessorHost.prototype.checkpointWorker = function(cb) {
  // Write a file named with our GUID. Other workers can use the last-modified time
  // to count the number of active workers
  console.log("checkpoint worker");
  var blob = path.join(this.hubName, "worker." + this.id);
  this.blobService.createBlockBlobFromText(this.container, blob, this.id, (err, result) => {
    if (cb) cb(err, result);
  });
}

EventProcessorHost.prototype._getPartitionOffset = function(partition, cb) {
  var blob = path.join(this.hubName, partition);
  this.blobService.getBlobToText(this.container, blob, (err, result) => {
    var checkpoint = JSON.parse(result);
    var offset = parseInt(checkpoint.offset);
    cb(err, offset);
  });
}

EventProcessorHost.prototype.checkpointPartition = function(partition, cb) {

  // Checkpoint the furthest event we've processed into the partition lock file

  if (this.latestOffsets == null) {
    return;
  }

  var blob = path.join(this.hubName, partition);
  var content = JSON.stringify({partition:partition, offset:this.latestOffsets[partition]});
  console.log("checkpoint partition: " + content);

  var opt = { leaseId : this.currentPartitions[partition].leaseId };
  this.blobService.createBlockBlobFromText(this.container, blob, content, opt, (err, result) => {
    if (err) { console.warn(err); }
    if (cb) cb(err, result);
  });
}

EventProcessorHost.prototype._tick = function() {

  // Main loop:
  // If we own less partitions than we think we should: attempt to acquire a new partition

  var self = this;
  var numPartitions = this._calcPartitionShare();

  if (Object.keys(this.currentPartitions).length < numPartitions) {

    // We have less partitions than we should, attempt to acquire another
    this._acquirePartition()
    .then((partition) => {
      if (partition) {

        // We succesfully acquired a new partition
        console.log(this.id + ":acquired " + partition);

        // Renew every half lease duration.
        // Note: This is a single shot that we re-set every time
        // we renew a lease
        setTimeout(() => {
          self._renewLease(partition);
        }, (self.leaseDuration / 2) * 1000);

        // Set up a checkpoint timer, we'll cancel this when
        // we fail to renew a lease
        self.currentPartitions[partition].leaseTimer = setInterval(() => {
          self.checkpointPartition(partition);
        }, self.checkpointInterval * 1000);


        if (self.latestOffsets == null) {
          self.latestOffsets = {};
        }

        self._getPartitionOffset(partition, (err, result) => {

          self.latestOffsets[partition] = result;

          // Start receiving messages on this partition
          var options = { startAfterOffset: result };
          self.currentPartitions[partition].receiver =
          self.eventHubClient.createReceiver(self.consumerGroup, partition, options)
          .then((rx) => {
            rx.on("errorReceived", (err) => {
              console.warn("error:" + err);
            });
            rx.on("message", (msg) => {
              self.latestOffsets[partition] = msg.systemProperties['x-opt-offset'];
              if (self.evp != null) {
                self.evp(msg);
              }
            });
          });
        });

      } else {
        // We didn't acquire a partition, see if we should break some leases
        self._breakPartitions();
      }
    })
    .catch((e) => {
      console.warn(e.stack);
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
        self.currentPartitions[partition] = { leaseId : result.id };
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

EventProcessorHost.prototype._breakPartitions = function() {

  // Break leases on the number of extra partitions we think we should have

  if (this.lastBreakTime != null && Date.now() - this.lastBreakTime < this.leaseDuration * 1000) {
    return;
  }

  var self = this;
  function breakLease(partition) {
    console.log("breakLease:" + partition);
    var blob = path.join(self.hubName, partition);
    self.blobService.breakLease(self.container, blob, (err, result) => {
    });
  }

  var numPartitions = this._calcPartitionShare();
  var toBreak = numPartitions - Object.keys(this.currentPartitions).length;
  if (toBreak > 0) {

    // breakPartition == partitions we don't currently lease
    var breakPartitions = Object.keys(this.partitions).filter(
      (x) => { return !(x in this.currentPartitions); }
    );

    // Pick random toBreak partitions to break
    breakPartitions = shuffle(breakPartitions).slice(0, toBreak);

    // Now break those leases..
    this.lastBreakTime = Date.now();
    for (var partition in breakPartitions) {
      breakLease(partition);
    }
  }
}

EventProcessorHost.prototype._renewLease = function(partition) {

  var blob = path.join(this.hubName, partition);
  var leaseId = this.currentPartitions[partition].leaseId;

  this.blobService.renewLease(this.container, blob, leaseId, (err, result) => {
    if (err) {
      // Lease has been broken, shutdown received, cancel the checkpoint timer,
      // write current checkpoint, release partition

      this.currentPartitions[partition].receiver.close();
      this.currentPartitions[partition].receiver = null;

      clearInterval(this.currentPartitions[partition].leaseTimer);
      this.checkpointPartition(partition, (err, result) => {
        this._releasePartition(partition);
      });
    }
    else {
      // Succesfully renewed the lease, business as usual, set a new
      // renew timer
      setTimeout(() => {
        this._renewLease(partition);
      }, (this.leaseDuration / 2) * 1000);
    }
  });
}

EventProcessorHost.prototype._releasePartition = function(partition) {

  var blob = path.join(this.hubName, partition);
  var leaseId = this.currentPartitions[partition].leaseId;
  this.blobService.releaseLease(this.container, blob, leaseId, (err, result) => {
    // Remove from current set after a delay to ensure we don't immediately
    // reacquire the partition
    setTimeout(() => {
      delete this.currentPartitions[partition];
    }, this.leaseDuration);
  });
}

EventProcessorHost.prototype._countWorkers = function() {

  var CULL_AFTER = 5 * 60; // Cull worker lock files that haven't been modified in 5 minutes

  var now = Date.now();
  var activeWorkers = [];
  var prefix = this.hubName + "/worker.";

  // Count the number of recently modified worker lock files..

  this.blobService.listBlobsSegmentedWithPrefix(this.container, prefix, null, (err, result) => {
    for (var entry of result.entries) {

      var lastModified = Date.parse(entry.properties["last-modified"]);
      var age = Date.now() - lastModified;

      // Tidy up old files..
      if (age > CULL_AFTER * 1000) {
        this.blobService.deleteBlob(this.container, entry.name, (err, result) => {
        });
      }
      else {

        // Worker appears active
        if (age < this.workerActiveTimeout * 1000) {
          activeWorkers.push(entry.name);
        }

      }
    }

    this.activeWorkers = activeWorkers;
    console.log("count workers:" + this.activeWorkers.length);
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
        if (err) reject(err);
        else resolve(result);
      });
    });
  });
}

module.exports = EventProcessorHost;
