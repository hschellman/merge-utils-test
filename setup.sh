#!/bin/bash

source /cvmfs/larsoft.opensciencegrid.org/spack-packages/setup-env.sh 
spack load r-m-dd-config experiment=dune
spack load justin
export SAM_EXPERIMENT=dune
#spack load kx509
#kx509
export ROLE=Analysis
voms-proxy-init -rfc -noregen -voms=dune:/dune/Role=$ROLE -valid 120:00
htgettoken -a htvaultprod.fnal.gov -i dune
