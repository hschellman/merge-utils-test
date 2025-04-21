"""Utility functions for interacting with the MetaCat web API."""
from __future__ import annotations

import collections
import logging

import metacat.webapi as metacat

from merge_utils import io_utils
from merge_utils.merge_set import MergeSet

logger = logging.getLogger(__name__)

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

def find_logical_files(query: str = None, filelist: list = None, config: dict = None) -> MergeSet:
    """
    Retrieve logical file information from MetaCat based on an MQL query or a list of DIDs.
    Returns a MergeSet of unique files if the metadata is consistent, otherwise an empty MergeSet.
    The config dictionary can be used to set the following options:
    - allow_missing: allow missing files
    - allow_duplicates: allow duplicate files
    - checked_fields: list of metadata fields to check for consistency
    """
    logger.debug("Retrieving logical files from MetaCat")

    if config is None:
        config = io_utils.read_config()['validation']

    if query is not None and filelist is not None and len(filelist) > 0:
        logger.warning("Both query and file list provided, was this intended?")

    mc_client = metacat.MetaCatClient()
    files = MergeSet()
    missing = collections.defaultdict(int)

    if query is not None:
        try:
            res = mc_client.query(query, with_metadata = True)
        except metacat.webapi.BadRequestError as err:
            logger.error("Malformed MetaCat query:\n  %s\n%s", query, err)
            return MergeSet()
        for file in res:
            files.add(file)

    if filelist is not None and len(filelist) > 0:
        didlist = [{'did':did} for did in filelist]
        try:
            res = mc_client.get_files(didlist, with_metadata = True)
        except (ValueError, metacat.webapi.BadRequestError) as err:
            logger.error("%s", err)
            return MergeSet()
        for file in res:
            files.add(file)
        for did in (x for x in filelist if x not in files):
            missing[did] += 1

    n_missing = log_bad_files(missing, "No MetaCat entry found for {count} {files}:")
    n_dupes = log_bad_files(files.dupes, "Found {count} duplicate {files}:")
    if n_missing and not config['allow_missing']:
        logger.error("Validation failed due to missing files")
        return MergeSet()
    if n_dupes and not config['allow_duplicates']:
        logger.error("Validation failed due to duplicate files")
        return MergeSet()

    if not files.check_consistency(config['checked_fields']):
        logger.error("Validation failed due to inconsistent metadata")
        return MergeSet()

    return files
