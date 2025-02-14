#!/usr/bin/env python3

"""
Script to take a metacat query or list of file DIDs, and then
1. Check for duplicate files in the list
2. Validate the metadata for those input files is ok
3. Ensure that the metadata of the input files is consistent for fields:
    namespace
    core.run_type
    core.file_type
    core.file_format
    core.data_tier
    core.data_stream
    core.application.name
    dune.campaign
    dune.requestid
    DUNE.requestid
4. If strict mode is enabled, additionally check consistency for fields:
    dune.config_file
    core.application.version
"""
##
# @mainpage ValidateMetadata
#
# @section description_main
#
#
# @file ValidateMetadata.py

import collections
import argparse
import logging

import metacat.webapi as metacat

import utils.io_utils
import logs.log

logger = logging.getLogger(__name__)

CHECKED_FIELDS = [
    "core.run_type",
    "core.file_type",
    "core.file_format",
    "core.data_tier",
    "core.data_stream",
    "core.application.name",
    "dune.campaign",
    "dune.requestid",
    "DUNE.requestid",
]
CHECKED_FIELDS_STRICT = [
    "dune.config_file",
    "core.application.version",
]

class UniqueFileList(collections.UserList):
    """Class to keep track of unique files"""

    def __init__(self, initlist=None):
        super().__init__(initlist)
        self.counts = {}

    def add(self, file: dict) -> None:
        """Add a file to the list if it is not there already"""
        did = file['namespace'] + ':' + file['name']

        if did not in self.counts:
            self.counts[did] = 1
            self.data.append(file)
            logger.debug("Added file %s", did)
        else:
            self.counts[did] += 1
            logger.debug("Duped file %s", did)

    def dupes(self) -> dict:
        """Get the list of files that are duplicated"""
        return {did:(count-1) for did, count in self.counts.items() if count > 1}

    def __contains__(self, did: str) -> bool:
        return did in self.counts


def get_checked_fields(file, strict=False) -> dict:
    """Get the list of fields to check for a file"""

    fields = collections.defaultdict(str)
    fields['namespace'] = file['namespace']

    for field in CHECKED_FIELDS:
        if field in file['metadata']:
            fields[field] = file['metadata'][field]

    if strict:
        for field in CHECKED_FIELDS_STRICT:
            if field in file['metadata']:
                fields[field] = file['metadata'][field]

    return fields

def check_consistency(files: list, strict=False) -> bool:
    """Check the consistency of the metadata for a list of files"""
    logger.debug("Checking metadata consistency")

    if len(files) < 2:
        return True

    consistent = True
    loosely_consistent = True
    errs = [""]
    fields = get_checked_fields(files[0], strict)
    for file in files[1:]:
        new_fields = get_checked_fields(file, strict)
        if fields != new_fields:
            consistent = False
            errs.append(f"\n  {file['namespace']}:{file['name']}")
            for key, value1 in fields.items():
                value2 = new_fields[key]
                if value1 != value2:
                    errs.append(f"\n    {key}: '{value1}' != '{value2}'")
                    if key not in CHECKED_FIELDS_STRICT:
                        loosely_consistent = False

    if not consistent:
        if strict and loosely_consistent:
            errs[0] = "File metadata failed strict consistency checks:"
        else:
            errs[0] = "File metadata is not consistent:"
        logger.error("".join(errs))

    return consistent

def log_bad_files(files: dict, msg: str) -> int:
    """Log a message for missing or duplicate files"""
    total = sum(files.values())
    if total == 0:
        return 0
    if total == 1:
        msg = [msg.format(count=1, files="file")]
    else:
        msg = [msg.format(count=total, files="files")]
    msg += [f"\n  ({count}) {file}" for file, count in files.items()]
    logger.warning("".join(msg))
    return total

def validate(query=None, filelist=None, strict=False, allow=False) -> list[str]:
    """Validate the metadata for a list of input files"""
    logger.debug("Validating metadata")

    mc_client = metacat.MetaCatClient()
    files = UniqueFileList()
    missing = collections.defaultdict(int)

    if filelist is not None and len(filelist) > 0:
        didlist = [{'did':did} for did in filelist]
        try:
            res = mc_client.get_files(didlist, with_metadata = True)
        except (ValueError, metacat.webapi.BadRequestError) as err:
            logger.error("%s", err)
            return []
        for file in res:
            files.add(file)
        for did in filelist:
            if did not in files:
                missing[did] += 1

    if query is not None:
        try:
            res = mc_client.query(query, with_metadata = True)
        except metacat.webapi.BadRequestError as err:
            logger.error("Malformed MetaCat query:\n  %s\n%s", query, err)
            return []
        for file in res:
            files.add(file)

    n_missing = log_bad_files(missing, "No MetaCat entry found for {count} {files}:")
    n_dupes = log_bad_files(files.dupes(), "Found {count} duplicate {files}:")
    if not allow and (n_missing > 0 or n_dupes > 0):
        logger.error("Validation failed due to missing or duplicate files")
        return []

    if not check_consistency(files, strict):
        return []

    return [file['namespace'] + ':' + file['name'] for file in files]


def main():
    """Main function for command line execution"""

    logs.log.setup()

    parser = argparse.ArgumentParser(
        description='validate metadata consistency and return a list of unique file DIDs')
    parser.add_argument('-q', '--query', help='MetaCat query to find files')
    parser.add_argument('-f', '--filelist', help='a file containing a list of file DIDs')
    parser.add_argument('-s', '--strict', action='store_true',
                        help='check additional fields for consistency')
    parser.add_argument('-a', '--allow', action='store_true',
                        help='allow missing or duplicate files')
    parser.add_argument('-d', '--debug', action='store_true', help='print debug information')
    parser.add_argument('files', nargs=argparse.REMAINDER, help='individual file DIDs')
    args = parser.parse_args()

    if args.debug:
        logs.log.override_level("DEBUG")

    flist = utils.io_utils.get_inputs(args.filelist, args.files)

    flist = validate(query=args.query, filelist=flist, strict=args.strict, allow=args.allow)
    for did in flist:
        print (did)

if __name__ == '__main__':
    main()
