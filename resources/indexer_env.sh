#!/bin/bash
eval `/cvmfs/icecube.opensciencegrid.org/py3-v4.1.0/setup.sh`
. /home/eevans/env-fc-indexer/bin/activate
$SROOT/metaprojects/combo/stable/env-shell.sh $@
