"""FileRetriever classes"""

import logging
import math
import collections
import asyncio
from abc import ABC, abstractmethod

from typing import AsyncGenerator, Generator

from merge_utils import io_utils, config
from merge_utils.merge_set import MergeSet, MergeChunk

logger = logging.getLogger(__name__)

class FileRetriever(ABC):
    """Base class for retrieving metadata from a source"""

    def __init__(self) -> dict:
        """Initialize the file retriever"""
        self.step = config.validation['batch_size']
        self.allow_missing = config.validation['allow_missing']
        self._missing = collections.defaultdict(int)
        self._files = MergeSet()

    @property
    def files(self) -> MergeSet:
        """Return the set of files from the source"""
        return self._files

    @property
    def dupes(self) -> dict:
        """Return the set of duplicate files from the source"""
        return self._files.dupes

    @property
    def missing(self) -> dict:
        """Return the set of missing files from the source"""
        return self._missing

    @abstractmethod
    async def connect(self) -> None:
        """Connect to the metadata source"""
        # connect to source

    async def add(self, files: list, dids: list = None) -> dict:
        """
        Add the metadata for a list of files to the set.
        
        :param files: list of dictionaries with file metadata
        :param dids: optional list of DIDs requested, used to check for missing files
        :return: dict of MergeFile objects that were added
        """
        # check for missing files
        if dids and len(files) < len(dids):
            res_set = {x['namespace'] + ':' + x['name'] for x in files}
            for did in set(dids) - res_set:
                self.missing[did] += 1
            if not self.allow_missing:
                io_utils.log_dict("No metadata found for {n} file{s}:", self.missing, logging.ERROR)
                raise ValueError("Missing file metadata")

        # add files to merge set
        added = await asyncio.to_thread(self.files.add_files, files)
        return added

    @abstractmethod
    async def input_batches(self) -> AsyncGenerator[dict, None]:
        """
        Asynchronously retrieve metadata for the next batch of files.

        :return: dict of MergeFile objects that were added
        """
        # yield batch

    async def _loop(self) -> None:
        """Repeatedly get input_batches until all files are retrieved."""
        # connect to source
        await self.connect()
        # loop over batches
        async for _ in self.input_batches():
            pass # do nothing

    def run(self) -> None:
        """Retrieve metadata for all files."""
        try:
            asyncio.run(self._loop())
        except ValueError as err:
            logger.error("%s", err)
            self.files = MergeSet()
            return

        # log any missing or duplicated files
        io_utils.log_dict("Missing metadata for {n} file{s}:", self.missing)
        io_utils.log_dict("Found {n} duplicate file{s}:", self.dupes)

    def output_chunks(self) -> Generator[MergeChunk, None, None]:
        """
        Yield chunks of files for merging.
        
        :return: yields a series of MergeChunk objects
        """
        for group in self.files.groups():
            if len(group) > config.merging['chunk_max']:
                n_chunks = math.ceil(len(group) / config.merging['chunk_max'])
                target_size = len(group) / n_chunks
                chunk = group.chunk()
                for file in sorted(group.values(), key=lambda f: f.path):
                    chunk.add(file)
                    if len(chunk) >= target_size:
                        yield chunk
                        chunk = group.chunk()
                if chunk:
                    yield chunk
            yield group
