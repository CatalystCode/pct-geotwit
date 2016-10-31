#!/bin/bash

# ----------------------
# KUDU Deployment Script
# Version: 0.2.2
# ----------------------

# Helpers
# -------

exitWithMessageOnError () {
  if [ ! $? -eq 0 ]; then
    echo "An error has occurred during web site deployment."
    echo $1
    exit 1
  fi
}

# Prerequisites
# -------------

# Verify node.js installed
hash node 2>/dev/null
exitWithMessageOnError "Missing node.js executable, please install node.js, if already installed make sure it can be reached from current environment."

# Setup
# -----

SCRIPT_DIR="${BASH_SOURCE[0]%\\*}"
SCRIPT_DIR="${SCRIPT_DIR%/*}"
ARTIFACTS=$SCRIPT_DIR/../artifacts
KUDU_SYNC_CMD=${KUDU_SYNC_CMD//\"}

if [[ ! -n "$DEPLOYMENT_SOURCE" ]]; then
  DEPLOYMENT_SOURCE=$SCRIPT_DIR
fi

if [[ ! -n "$NEXT_MANIFEST_PATH" ]]; then
  NEXT_MANIFEST_PATH=$ARTIFACTS/manifest

  if [[ ! -n "$PREVIOUS_MANIFEST_PATH" ]]; then
    PREVIOUS_MANIFEST_PATH=$NEXT_MANIFEST_PATH
  fi
fi

DEPLOYMENT_SOURCE_USER_GRAPH=$DEPLOYMENT_SOURCE\\jobs\\continuous\\build_user_graph
DEPLOYMENT_SOURCE_TWITTER_INGEST=$DEPLOYMENT_SOURCE\\jobs\\continuous\\twitter_ingest
DEPLOYMENT_SOURCE_INFER_LOCATION=$DEPLOYMENT_SOURCE\\jobs\\triggered\\infer_location
DEPLOYMENT_SOURCE_INFERENCE_CONTROLLER=$DEPLOYMENT_SOURCE\\jobs\\triggered\\inference_controller
DEPLOYMENT_SOURCE_TEMPLATE=$DEPLOYMENT_SOURCE\\pct-webjobtemplate\\lib\\azure-storage-tools


if [[ ! -n "$DEPLOYMENT_TARGET" ]]; then
  DEPLOYMENT_TARGET=$ARTIFACTS/wwwroot
else
  KUDU_SERVICE=true
fi

DEPLOYMENT_TARGET_WEBJOBS=$DEPLOYMENT_TARGET\\App_Data\\jobs

echo "deploy directory is $DEPLOYMENT_TARGET_WEBJOBS"
echo "deploy target is $DEPLOYMENT_TARGET"

DEPLOYMENT_SOURCE_WEBJOBS=$DEPLOYMENT_SOURCE/jobs

if [[ ! -n "$KUDU_SYNC_CMD" ]]; then
  # Install kudu sync
  echo Installing Kudu Sync
  npm install kudusync -g --silent
  exitWithMessageOnError "npm failed"

  if [[ ! -n "$KUDU_SERVICE" ]]; then
    # In case we are running locally this is the correct location of kuduSync
    KUDU_SYNC_CMD=kuduSync
  else
    # In case we are running on kudu service this is the correct location of kuduSync
    KUDU_SYNC_CMD=$APPDATA/npm/node_modules/kuduSync/bin/kuduSync
  fi
fi

# Node Helpers
# ------------

selectNodeVersion () {
  echo Selecting Node Version
  if [[ -n "$KUDU_SELECT_NODE_VERSION_CMD" ]]; then
    SELECT_NODE_VERSION="$KUDU_SELECT_NODE_VERSION_CMD \"$DEPLOYMENT_SOURCE\" \"$DEPLOYMENT_TARGET\" \"$DEPLOYMENT_TEMP\""
    eval $SELECT_NODE_VERSION
    exitWithMessageOnError "select node version failed"

    if [[ -e "$DEPLOYMENT_TEMP/__nodeVersion.tmp" ]]; then
      NODE_EXE=`cat "$DEPLOYMENT_TEMP/__nodeVersion.tmp"`
      exitWithMessageOnError "getting node version failed"
    fi

    if [[ -e "$DEPLOYMENT_TEMP/.tmp" ]]; then
      NPM_JS_PATH=`cat "$DEPLOYMENT_TEMP/__npmVersion.tmp"`
      exitWithMessageOnError "getting npm version failed"
    fi

    if [[ ! -n "$NODE_EXE" ]]; then
      NODE_EXE=node
    fi

    NPM_CMD="\"$NODE_EXE\" \"$NPM_JS_PATH\""
  else
    NPM_CMD=npm
    NODE_EXE=node
  fi
}

##################################################################################################################################
# Deployment
# ----------

echo Handling custom node.js deployment.

touch server.js

selectNodeVersion
eval $NPM_CMD cache clean 
echo "deploy source is $DEPLOYMENT_SOURCE_TEMPLATE"

if [ -e "$DEPLOYMENT_SOURCE_TEMPLATE\\package.json" ]; then
  cd $DEPLOYMENT_SOURCE_TEMPLATE
  echo Installing NPM Packages
  eval $NPM_CMD cache clean 
  eval $NPM_CMD install
  exitWithMessageOnError "npm failed"

  cd - > /dev/null
fi
echo "deploy source is $DEPLOYMENT_SOURCE_USER_GRAPH"

if [ -e "$DEPLOYMENT_SOURCE_USER_GRAPH\\package.json" ]; then
  cd $DEPLOYMENT_SOURCE_USER_GRAPH
  echo Installing NPM Packages
  eval $NPM_CMD cache clean 
  eval $NPM_CMD install
  exitWithMessageOnError "npm failed"

  cd - > /dev/null
fi

echo "deploy source is $DEPLOYMENT_SOURCE_TWITTER_INGEST"
if [ -e "$DEPLOYMENT_SOURCE_TWITTER_INGEST\\package.json" ]; then
  cd $DEPLOYMENT_SOURCE_TWITTER_INGEST
  echo Installing NPM Packages
  eval $NPM_CMD cache clean 
  eval $NPM_CMD install
  exitWithMessageOnError "npm failed"
  cd - > /dev/null
fi

echo "deploy source is $DEPLOYMENT_SOURCE_INFER_LOCATION"
if [ -e "$DEPLOYMENT_SOURCE_INFER_LOCATION\\package.json" ]; then
  cd $DEPLOYMENT_SOURCE_INFER_LOCATION
  echo Installing NPM Packages
  eval $NPM_CMD cache clean 
  eval $NPM_CMD install
  exitWithMessageOnError "npm failed"

echo "deploy source is $DEPLOYMENT_SOURCE_INFERENCE_CONTROLLER"
if [ -e "$DEPLOYMENT_SOURCE_INFERENCE_CONTROLLER\\package.json" ]; then
  cd $DEPLOYMENT_SOURCE_INFERENCE_CONTROLLER
  echo Installing NPM Packages
  eval $NPM_CMD cache clean 
  eval $NPM_CMD install
  exitWithMessageOnError "npm failed"

  cd - > /dev/null
fi

echo Syncing Files to $DEPLOYMENT_TARGET_WEBJOBS
"$KUDU_SYNC_CMD" -v 50 -f "$DEPLOYMENT_SOURCE_WEBJOBS" -t "$DEPLOYMENT_TARGET_WEBJOBS" -n "$NEXT_MANIFEST_PATH" -p "$PREVIOUS_MANIFEST_PATH" -i ".git;.hg;.deployment;deploy.sh"
exitWithMessageOnError "Kudu Sync failed"

##################################################################################################################################
# Post deployment stub
if [[ -n "$POST_DEPLOYMENT_ACTION" ]]; then
  POST_DEPLOYMENT_ACTION=${POST_DEPLOYMENT_ACTION//\"}
  cd "${POST_DEPLOYMENT_ACTION_DIR%\\*}"
  "$POST_DEPLOYMENT_ACTION"
  exitWithMessageOnError "post deployment action failed"
fi


##################################################################################################################################
echo "Finished successfully."
