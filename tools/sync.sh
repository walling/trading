#!/bin/bash

# Immediately exit on failure; work in root directory
set -e
cd $(dirname "$0")/..

# Sync files using rsync
rsync -chavzP --delete --stats trading-rsync@herkules:data .
