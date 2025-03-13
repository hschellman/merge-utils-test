"""Utility functions for interacting with the MetaCat web API."""

import collections
import logging

import metacat.webapi as metacat

from src.file_utils import DataFile, DataSet

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

def get_checked_fields(file: DataFile, strict=False) -> dict:
    """Get the list of fields to check for a file"""

    fields = {'namespace': file.namespace}

    for field in CHECKED_FIELDS:
        if field in file.metadata:
            fields[field] = file.metadata[field]

    if strict:
        for field in CHECKED_FIELDS_STRICT:
            if field in file.metadata:
                fields[field] = file.metadata[field]

    return fields

def check_consistency(files: DataSet, strict=False) -> bool:
    """
    Check the consistency of the metadata for a list of files.
    By default, require consistency for CHECKED_FIELDS.
    If strict is True, also check CHECKED_FIELDS_STRICT.
    """
    logger.debug("Checking metadata consistency")

    if len(files) < 2:
        return True

    consistent = True
    loosely_consistent = True
    errs = [""]
    fields = None
    for file in files:
        new_fields = get_checked_fields(file, strict)
        if fields is None:
            fields = new_fields
            continue
        if fields != new_fields:
            consistent = False
            errs.append(f"\n  {file.did}")
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
    msg += [f"\n  ({count}) {file}" for file, count in sorted(files.items())]
    logger.warning("".join(msg))
    return total

def find_logical_files(query=None, filelist=None, strict=False, allow=False) -> DataSet:
    """
    Retrieve logical file information from MetaCat based on an MQL query or a list of DIDs.
    Returns a list of unique files if the metadata is consistent, otherwise an empty list.
    By default, consistency is checked for CHECKED_FIELDS.
    If strict is True, also check CHECKED_FIELDS_STRICT.
    If allow is True, missing or duplicate files are allowed in the input.
    """
    logger.debug("Retrieving logical files from MetaCat")

    if filelist is None:
        filelist = []
    if query is not None and len(filelist) > 0:
        logger.warning("Both query and file list provided, was this intended?")

    mc_client = metacat.MetaCatClient()
    files = DataSet()
    missing = collections.defaultdict(int)

    if query is not None:
        try:
            res = mc_client.query(query, with_metadata = True)
        except metacat.webapi.BadRequestError as err:
            logger.error("Malformed MetaCat query:\n  %s\n%s", query, err)
            return DataSet()
        for file in res:
            files.add(file)

    if len(filelist) > 0:
        didlist = [{'did':did} for did in filelist]
        try:
            res = mc_client.get_files(didlist, with_metadata = True)
        except (ValueError, metacat.webapi.BadRequestError) as err:
            logger.error("%s", err)
            return DataSet()
        for file in res:
            files.add(file)
        for did in filelist:
            if did not in files:
                missing[did] += 1

    n_missing = log_bad_files(missing, "No MetaCat entry found for {count} {files}:")
    n_dupes = log_bad_files(files.dupes(), "Found {count} duplicate {files}:")
    if not allow and (n_missing > 0 or n_dupes > 0):
        logger.error("Validation failed due to missing or duplicate files")
        return DataSet()

    if not check_consistency(files, strict):
        return DataSet()

    return files
