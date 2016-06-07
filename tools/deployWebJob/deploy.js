var os = require("os")
var fs = require("fs")
var path = require("path")
var azure = require("azure")
var archiver = require('archiver')
var randomstring = require("randomstring")

function usage() {
  console.log("Usage:\n\tnode deploy.js <path_to_package>\n\n")
}

function main(argv) {

  /* This is all tediously necessary since npm@3 now flattens
   * node_modules which is good news for Windows users but makes
   * creating the package zip file for a web job that much
   * more labourious (since we can't just locally install, zip
   * and upload any more)
   */

  if (argv.length < 3) {
    usage();
    process.exit(1);
  }

  // Get package files
  var packageDir = argv[2]
  var packageJson = JSON.parse(
    fs.readFileSync(path.join(packageDir, "package.json"), "utf-8")
  )
  var files = packageJson.files

  //var tempDir = path.join(os.tmpdir(), randomstring.generate())
  //fs.mkdirSync(tempDir)
  var output = fs.createWriteStream('file.zip')
  var archive = archiver('zip')
  archive.pipe(output)
  for (file of files) {
    archive.file(path.join(packageDir, file), {name:file})
  }
  archive.directory(path.join(packageDir, "node_modules"), "node_modules")
  archive.finalize()
}

if (require.main === module) {
    main(process.argv);
}
