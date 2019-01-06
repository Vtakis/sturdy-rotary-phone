#!/bin/bash
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

WORKLOAD_DIR=${1-$DIR/input-files/public}
WORKLOAD_DIR=$(echo $WORKLOAD_DIR | sed 's:/*$::')

cd $WORKLOAD_DIR
WORKLOAD=$(basename "$PWD")
echo execute $WORKLOAD ...
$DIR/harness *.init *.work *.result ../../run.sh
