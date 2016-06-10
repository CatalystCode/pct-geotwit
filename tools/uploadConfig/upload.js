var fs = require("fs");
var exec = require("child_process").execSync

function shell(cmd, args, cb) {

  console.log("\n" + cmd + " " + args.join(" "))

  // Would rather use spawn here but on Windows spawn wont start a
  // .cmd (which is what the azure cli is).
  // This'll run the Windows version of azure.cmd, not any npm
  // installed version, btw.

  console.log(exec(cmd + " " + args.join(" ")).toString());
}

function main(argv) {
  var config = JSON.parse(fs.readFileSync(argv[0]), "utf-8");
  for (var key in config) {
    shell("azure", ["site", "appsetting", "delete", "-q", key,  argv[1]]);
    shell("azure", ["site", "appsetting", "add", key + "=" + config[key], argv[1]]);
  }
}

if (require.main === module) {
    main(process.argv.slice(2));
}
