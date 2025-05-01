"""Utility functions for interacting with the Rucio web API."""
from __future__ import annotations

import logging
import asyncio

from rucio.client.replicaclient import ReplicaClient

from merge_utils.merge_set import MergeFile, MergeSet, FileRetriever
from merge_utils.merge_rse import MergeRSEs
from merge_utils import io_utils
from merge_utils import metacat_utils

logger = logging.getLogger(__name__)

class RucioRetriever:
    """Class for managing asynchronous queries to the Rucio web API."""

    def __init__(self, source: FileRetriever, config: dict = None):
        """
        Initialize the RucioRetriever with a configuration file.

        :param config: configuration dictionary
        """
        if config is None:
            config = io_utils.read_config()
        self.checksums = config['validation']['checksums']
        self.rses = MergeRSEs(config=config)
        self.source = source

        self.client = None

    @property
    def files(self) -> MergeSet:
        """Return the set of files from the source"""
        return self.source.files

    @property
    def missing(self) -> dict:
        """Return the set of missing files from the source"""
        return self.source.missing

    @property
    def dupes(self) -> dict:
        """Return the set of duplicate files from the source"""
        return self.source.dupes

    async def connect(self) -> None:
        """Connect to the Rucio web API"""
        if not self.client:
            logger.debug("Connecting to Rucio")
            src_connect = asyncio.create_task(self.source.connect())
            rse_connect = asyncio.create_task(self.rses.connect())
            self.client = ReplicaClient()
            await rse_connect
            await src_connect
        else:
            logger.debug("Already connected to Rucio")

    async def checksum(self, file: MergeFile, rucio: dict) -> bool:
        """
        Ensure file sizes and checksums from MetaCat and Rucio agree
        
        :param file: MergeFile object to check
        :param rucio: Rucio replicas dictionary
        :return: True if checksums match, False otherwise
        """
        # Check the file size
        if file.size != rucio['bytes']:
            logger.error("Size mismatch for %s: %d != %d", file.did, file.size, rucio['bytes'])
            return False
        # See if we should skip the checksum check
        if len(self.checksums) == 0:
            return True
        # Check the checksum
        for algo in self.checksums:
            if algo in file.checksums and algo in rucio:
                csum1 = file.checksums[algo]
                csum2 = rucio[algo]
                if csum1 == csum2:
                    logger.debug("Found matching %s checksum for %s", algo, file.did)
                    return True
                logger.error("%s checksum mismatch for %s: %s != %s", algo, file.did, csum1, csum2)
                return False
            if algo not in file.checksums:
                logger.debug("MetaCat missing %s checksum for %s", algo, file.did)
            if algo not in rucio:
                logger.debug("Rucio missing %s checksum for %s", algo, file.did)
        # If we get here, we have no matching checksums
        logger.error("No matching checksums for %s", file.did)
        return False

    async def process(self, files: dict) -> None:
        """
        Process a batch of files to find their physical locations in Rucio.
        
        :param files: dictionary of files to process
        """
        logger.debug("Retrieving physical file paths from Rucio")
        found = set()
        query = [{'scope':file.namespace, 'name':file.name} for file in files.values()]
        res = await asyncio.to_thread(self.client.list_replicas, query, ignore_availability=False)
        for replicas in res:
            did = replicas['scope'] + ':' + replicas['name']
            logger.debug("Found %d replicas for %s", len(replicas['pfns']), did)
            found.add(did)
            file = files[did]

            added, csum = await asyncio.gather(
                self.rses.add_replicas(did, replicas),
                self.checksum(file, replicas)
            )

            if not csum:
                raise ValueError(f"Checksum mismatch for {file.did}")

            logger.debug("Added %d replicas for %s", added, did)

        missing = [file.did for file in files if file not in found]
        errs = io_utils.log_list("No Rucio entry for {n} file{s}:", missing, logging.ERROR)
        if errs:
            raise ValueError("Failed to find all files in Rucio!")

    async def _loop(self) -> None:
        """Repeatedly process batches from the source until all files are retrieved."""
        # connect to source
        await self.connect()
        # loop over batches
        async for files in self.source.next_batch():
            await self.process(files)

    def run(self) -> MergeRSEs:
        """
        Retrieve metadata for all files.

        :return: MergeSet of all files
        """
        logger.debug("Retrieving physical file paths from Rucio")
        try:
            asyncio.run(self._loop())
        except ValueError as err:
            logger.error("%s", err)
            return MergeRSEs()

        # log any missing or duplicated files
        io_utils.log_dict("Missing metadata for {n} file{s}:", self.missing)
        io_utils.log_dict("Found {n} duplicate file{s}:", self.dupes)

        self.rses.cleanup()
        return self.rses




def find_physical_files(query: str = None, filelist: list = None, config: dict = None) -> MergeRSEs:
    """Get the best physical locations for a list of logical files"""
    logger.debug("Retrieving physical file paths from Rucio")

    retriever = RucioRetriever(
        source = metacat_utils.MetaCatRetriever(query, filelist, config),
        config = config
    )

    rses = retriever.run()
    return rses
