#!/bin/bash

source /cvmfs/larsoft.opensciencegrid.org/spack-packages/setup-env.sh 
spack load r-m-dd-config experiment=dune
spack load kx509
export SAM_EXPERIMENT=dune
kx509
export ROLE=Analysis
voms-proxy-init -rfc -noregen -voms=dune:/dune/Role=$ROLE -valid 120:00
