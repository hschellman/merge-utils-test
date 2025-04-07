#!/usr/bin/env python3

"""
Script to take a metacat query or list of file DIDs, and then:
1. Find the logical file entries in MetaCat and validate the metadata
2. Retrieve the list of physical files from Rucio
"""
##
# @mainpage locate_files
#
# @section description_main
#
#
# @file locate_files.py

import argparse
import logging

from src import io_utils, metacat_utils, rucio_utils

logger = logging.getLogger(__name__)

def main():
    """Main function for command line execution"""

    io_utils.setup_log(__file__)

    parser = argparse.ArgumentParser(
        description='validate metadata consistency and return a list of unique file DIDs')
    parser.add_argument('-q', '--query', help='MetaCat query to find files')
    parser.add_argument('-f', '--filelist', help='a file containing a list of file DIDs')
    parser.add_argument('-c', '--config', help='a configuration file')
    parser.add_argument('-d', '--debug', action='store_true', help='print debug information')
    parser.add_argument('files', nargs=argparse.REMAINDER, help='individual file DIDs')
    args = parser.parse_args()

    if args.debug:
        io_utils.set_log_level("DEBUG")

    config = io_utils.read_config(args.config)

    flist = io_utils.get_inputs(args.filelist, args.files)

    flist = metacat_utils.find_logical_files(
        query=args.query, filelist=flist, config=config['validation']
    )

    flist = rucio_utils.find_physial_files(flist)
    for file in flist:
        print (file.paths)

if __name__ == '__main__':
    main()
