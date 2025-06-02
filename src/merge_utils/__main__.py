"""Command line interface for merge_utils."""

import argparse
import logging

from merge_utils import io_utils, config, metacat_utils, rucio_utils, scheduler

logger = logging.getLogger(__name__)

def main():
    """Test the command line interface for merge_utils."""

    parser = argparse.ArgumentParser(
        description='Command line interface for merge_utils')
    #parser.add_argument("function", choices=["list_dids", "list_pfns"], help="Function to execute")

    subparsers = parser.add_subparsers(metavar='FUNCTION', dest='function', help="function to execute")

    did_parser = subparsers.add_parser('dids', help='list the DIDs of files from MetaCat')
    did_parser.add_argument('-q', '--query', help='MetaCat query to find files')
    did_parser.add_argument('-f', '--filelist', help='a file containing a list of file DIDs')
    did_parser.add_argument('files', nargs=argparse.REMAINDER, help='individual file DIDs')

    pfn_parser = subparsers.add_parser('pfns', help='list the PFNs of files from Rucio')
    pfn_parser.add_argument('-q', '--query', help='MetaCat query to find files')
    pfn_parser.add_argument('-f', '--filelist', help='a file containing a list of file DIDs')
    pfn_parser.add_argument('-a', '--all', action='store_true', help='list replicas from all RSEs')
    pfn_parser.add_argument('files', nargs=argparse.REMAINDER, help='individual file DIDs')

    merge_parser = subparsers.add_parser('merge', help='actualy merge files')
    merge_parser.add_argument('-q', '--query', help='MetaCat query to find files')
    merge_parser.add_argument('-f', '--filelist', help='a file containing a list of file DIDs')
    merge_parser.add_argument('files', nargs=argparse.REMAINDER, help='individual file DIDs')

    parser.add_argument('-c', '--config', help='a configuration file')
    parser.add_argument('-v', '--verbose', action='count', default=0, help='print more verbose output')

    args = parser.parse_args()
    io_utils.setup_log(args.function)
    config.load(args.config)
    if args.verbose == 1:
        io_utils.set_log_level("INFO")
    elif args.verbose > 1:
        io_utils.set_log_level("DEBUG")

    if args.function == "dids":
        list_dids(args)
    elif args.function == "pfns":
        list_pfns(args)
    elif args.function == "merge":
        merge(args)


def list_dids(args):
    """List the DIDs of files from MetaCat"""

    flist = io_utils.get_inputs(args.filelist, args.files)

    retriever = metacat_utils.MetaCatRetriever(query=args.query, filelist=flist)
    retriever.run()
    for file in retriever.files:
        print (file.did)

def list_pfns(args):
    """List the PFNs of files from Rucio"""
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

def merge(args):
    """List the PFNs of files from Rucio"""
    flist = io_utils.get_inputs(args.filelist, args.files)

    sched = scheduler.JustinScheduler(rucio_utils.RucioRetriever(
        metacat_utils.MetaCatRetriever(query=args.query, filelist=flist)
    ))
    sched.run()
