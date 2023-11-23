#!/bin/bash

pushd ./tools/csd_fuse
./init_fuse.sh $1 $2 $3
popd
