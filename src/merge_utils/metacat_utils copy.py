"""Utility functions for interacting with the MetaCat web API."""
from __future__ import annotations

import collections
import logging
import itertools
import asyncio

import metacat.webapi as metacat

from . import io_utils
from .merge_set import MergeSet

logger = logging.getLogger(__name__)

chunk_size = 100

async def mc_query(client: metacat.MetaCatClient, query: str, chunk: int) -> list:
    """
    Asynchronous function to query MetaCat and return the results.

    :param client: MetaCatClient instance
    :param query: MQL query string
    :return: list of results from the query
    """

    logger.debug("Querying MetaCat for chunk #%d", chunk)
    query_chunk = query + f" skip {chunk * chunk_size} limit {chunk_size}"
    try:
        res = client.query(query_chunk, with_metadata = True)
    except metacat.webapi.BadRequestError as err:
        logger.error("Malformed MetaCat query:\n  %s\n%s", query, err)
        return []
    return res

def find_logical_files(query: str = None, filelist: list = None, config: dict = None) -> MergeSet:
    """
    Retrieve logical file information from MetaCat based on an MQL query or a list of DIDs.

    :param query: MQL query to find files
    :param filelist: list of file DIDs to find
    :param config: configuration dictionary with options for validation, with the following keys:
        - allow_missing: allow missing files
        - allow_duplicates: allow duplicate files
        - checked_fields: list of metadata fields to check for consistency
    :return: MergeSet of unique files if the metadata is consistent, otherwise an empty MergeSet.
    """
    logger.debug("Retrieving logical file metadata from MetaCat")

    if config is None:
        config = io_utils.read_config()['validation']

    if query is not None and filelist is not None and len(filelist) > 0:
        logger.warning("Both query and file list provided, was this intended?")

    mc_client = metacat.MetaCatClient()
    files = MergeSet()
    missing = collections.defaultdict(int)

    if query is not None:
        chunk_size = 100
        for i in itertools.count():
            logger.debug("Querying MetaCat for chunk #%d", i)
            query_chunk = query + f" skip {i * chunk_size} limit {chunk_size}"
            try:
                res = mc_client.query(query_chunk, with_metadata = True)
            except metacat.webapi.BadRequestError as err:
                logger.error("Malformed MetaCat query:\n  %s\n%s", query, err)
                return MergeSet()
            n_res = 0
            for file in res:
                n_res += 1
                files.add(file)
            logger.debug("MetaCat query returned %d files", n_res)
            if n_res < chunk_size:
                break

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

    n_missing = io_utils.log_dict("No MetaCat entry found for {n} file{s}:", missing)
    n_dupes = io_utils.log_dict("Found {n} duplicate file{s}:", files.dupes)
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
