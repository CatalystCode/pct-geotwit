var path = require("path");
var azure = require("azure");
var EventHubClient = require("azure-event-hubs").Client;

function EventProcessorHost(blobService, eventHubConnectionString)
{
  this.blobService = blobService;

  this.container = "eph";
  this.leaseDuration = 60;
  this.hubConnectionString = eventHubConnectionString;

  var stringParts = eventHubConnectionString.split("=");
  this.hubName = stringParts[stringParts.length - 1];

  this.partitions = [];
  this.currentPartitions = {};

  this.eventHubClient = new EventHubClient.fromConnectionString(eventHubConnectionString);
}

EventProcessorHost.prototype.init = function() {
  return this.eventHubClient.getPartitionIds()
  .then((partitions) => {
    this.partitions = partitions;
    return this._initPartitionBlobs();
  });
}

EventProcessorHost.prototype._initPartitionBlobs = function() {

  /* Make sure we have exactly the blob files we need to coordinate
   * multiple jobs across the partitions. If we've got what we need then
   * we're in the middle of a run. If we don't then create fresh ones
   */

  var containerLeaseId = null;
  return new Promise((resolve, reject) => {
    console.log("a");
    // Create the container (doesn't matter if it already exists)
    this.blobService.createContainer(this.container, (err, result) => {
      console.log(1);
      resolve(err != null);
    });
  })
  .then(() => {
    // Mutex the container whilst we create our blobs
    return new Promise((resolve, reject) => {
      console.log("b");
      var options = { leaseDuration: this.leaseDuration };
      this.blobService.acquireLease(this.container, null, options, (err, result) => {
        if (err) reject(err);
        else {
          console.log(2);
          resolve(result);
        }
      });
    });
  })
  .then((result) => {

    // List contents of container
    containerLeaseId = result.id;
    return new Promise((resolve, reject) => {
      console.log("c");
      this.blobService.listBlobsSegmentedWithPrefix(
        this.container, this.hubName, null, (err, result) => {
        if (err) reject(err);
        else {
          console.log(3);
          resolve(result.entries.map((x) => { return x.name; }));
        }
      });
    });
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
          console.log("d");
          this.blobService.createBlockBlobFromText(this.container, blob, content, (err, result) => {
            if (err) reject(err);
            else {
              console.log(4);
              resolve(result);
            }
          });
        }));
      };
    }
    console.log(all);
    return all.length > 0 ? Promise.all(all) : [];
  })
  .then((result) => {

  });
}

EventProcessorHost.prototype.acquirePartition = function() {

  console.log("acquire");
  console.log(current);
  console.log(partitions);

  return new Promise((resolve, reject) => {

    if (current.length >= partitions.length) {
      resolve([]);
    }

    function tryPartition(partition) {
      var blob = path.join(blobRoot, "part" + partition);
      return new Promise((resolve, reject) => {
        blobService.acquireLease(container, blob, { leaseDuration: 60 }, (err, result) => {
          if (err) reject(err);
          else {
            resolve(partition);
          }
        });
      });
    }

    if (all.length > 0) {
      console.log("all: " + all.length);
      return Promise.all(all);
    }

    console.log("None");
    return [];
  });
}

EventProcessorHost.prototype.releasePartition = function(partition) {

  return new Promise((resolve, reject) => {
    var blob = path.join(blobRoot, "part" + partition);
    blobService.renewLease(container, blob, currentPartitions[partition], (err, result) => {
      if (err) reject(err);
      else {
        resolve(result);
        setTimeout(() => {
          delete currentPartitions[partition];
        }, 10000);
      }
    });
  });
}


module.exports = EventProcessorHost;
