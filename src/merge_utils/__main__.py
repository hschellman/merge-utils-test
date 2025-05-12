"""Command line interface for merge_utils."""

import argparse
import logging

from merge_utils import io_utils, config, metacat_utils, rucio_utils

logger = logging.getLogger(__name__)

def main():
    """Test the command line interface for merge_utils."""

    parser = argparse.ArgumentParser(
        description='Command line interface for merge_utils')
    parser.add_argument("function", choices=["list_dids", "list_pfns"], help="Function to execute")
    parser.add_argument('-c', '--config', help='a configuration file')
    parser.add_argument('-d', '--debug', action='store_true', help='print debug information')

    args, rest = parser.parse_known_args()
    io_utils.setup_log(args.function)
    config.load(args.config)
    if args.debug:
        io_utils.set_log_level("DEBUG")

    if args.function == "list_dids":
        list_dids(rest)
    elif args.function == "list_pfns":
        list_pfns(rest)


def list_dids(arg_list: list = None):
    """List the DIDs of files from MetaCat"""

    parser = argparse.ArgumentParser(
        description='validate metadata consistency and return a list of unique file DIDs')
    parser.add_argument('-q', '--query', help='MetaCat query to find files')
    parser.add_argument('-f', '--filelist', help='a file containing a list of file DIDs')
    parser.add_argument('files', nargs=argparse.REMAINDER, help='individual file DIDs')

    if arg_list:
        args = parser.parse_args(arg_list)
    else:
        args = parser.parse_args()

    flist = io_utils.get_inputs(args.filelist, args.files)

    retriever = metacat_utils.MetaCatRetriever(query=args.query, filelist=flist)
    retriever.run()
    for file in retriever.files:
        print (file.did)

def list_pfns(arg_list: list = None):
    """List the PFNs of files from Rucio"""

    parser = argparse.ArgumentParser(
        description='validate metadata consistency and return a list of unique file DIDs')
    parser.add_argument('-q', '--query', help='MetaCat query to find files')
    parser.add_argument('-f', '--filelist', help='a file containing a list of file DIDs')
    parser.add_argument('-a', '--all', action='store_true', help='list replicas from all RSEs')
    parser.add_argument('files', nargs=argparse.REMAINDER, help='individual file DIDs')

    if arg_list:
        args = parser.parse_args(arg_list)
    else:
        args = parser.parse_args()

    flist = io_utils.get_inputs(args.filelist, args.files)

    retriever = rucio_utils.RucioRetriever(
        metacat_utils.MetaCatRetriever(query=args.query, filelist=flist)
    )
    retriever.run()

    if args.all:
        rses = retriever.rses
        for name, rse in rses.items():
            print(f"RSE {name}:")
            for pfn in rse.pfns.values():
                print(f"  {pfn}")
    else:
        for chunk in retriever.output_chunks():
            print(f"Output file {chunk.name}:")
            print(f"site {chunk.site}")
            for pfn in chunk.values():
                print(f"  {pfn.path}")
