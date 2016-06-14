var path = require("path");
var exec = require("child_process").exec;

/*&$AzCopyPath $rootPath\Data https://$StorageAccountName.blob.core.windows.net/$ReferenceContainerName /DestKey:$StorageKey *.* /Y */

AZCOPY_PATH = path.join(".", "tools", "AzCopy.exe");

function shell(cmd, args, cb) {

  console.log("\n" + cmd + " " + args.join(" "))

  // Would rather use spawn here but on Windows spawn wont start a
  // .cmd (which is what the azure cli is).
  // This'll run the Windows version of azure.cmd, not any npm
  // installed version, btw.
  cb(null);

  var shell = exec(cmd + " " + args.join(" "))
  shell.stdout.on("data", (data) => {
    process.stdout.write(`${data}`);
  });
  shell.stderr.on("data", (data) => {
    console.log(`exec stderr: ${data}`);
  });
  shell.on("close", (code) => {
    cb(code)
  });
}

function usage() {
  console.log("node run.js <source_dir> <container_name> " +
  "<storage_account_name> <storage_account_key>");
}

function main(argv) {

  if (argv.length < 4) {
    usage();
    process.exit(1);
  }

  var source = argv[0];
  var containerName = argv[1];
  var storageAccountName = argv[2];
  var storageAccountKey = argv[3];

  var dest = `https://${storageAccountName}.blob.core.windows.net/${containerName}`
  shell(AZCOPY_PATH, [source, dest, `/DestKey:${storageAccountKey}`, "*.*", "/Y", "/S"], (code) => {
    if (code == 0) {
      console.log("done");
    }
  });
}

if (require.main === module) {
    main(process.argv.slice(2));
}
