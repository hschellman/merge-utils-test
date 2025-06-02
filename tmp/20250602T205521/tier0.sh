#!/bin/bash
# This script will submit the JustIN jobs for tier 0
justin simple-workflow --monte-carlo 2 --jobscript /nashome/e/emuldoon/merge-utils/src/merge_utils/merge.jobscript --env MERGE_CONFIG="tier0_CERN" --env CONFIG_DIR="/cvmfs/fifeuser2.opensciencegrid.org/sw/dune/6bb16fb86136479f53a36d0cb0295be2c67d0c1b" --env OUT_DIR="/nashome/e/emuldoon/scratch" --site CERN
