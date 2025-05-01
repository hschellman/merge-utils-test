"""Container for a set of files to be merged"""

from __future__ import annotations
import collections
import logging
import subprocess
import hashlib
import asyncio
from typing import Iterable, AsyncGenerator
from abc import ABC, abstractmethod

from . import io_utils

logger = logging.getLogger(__name__)

class FileRetriever(ABC):
    """Base class for retrieving metadata from a source"""
    step: int
    allow_missing: bool
    missing: dict
    files: MergeSet

    def load_config(self, config: dict = None) -> dict:
        """
        Initialize the metadata retriever with a configuration.

        :param config: configuration dictionary
        """
        if config is None:
            config = io_utils.read_config()
        self.step = config['validation']['batch_size']
        self.allow_missing = config['validation']['allow_missing']
        self.missing = collections.defaultdict(int)
        self.files = MergeSet(config=config)

        return config

    @property
    def dupes(self) -> dict:
        """Return the set of duplicate files from the source"""
        return self.files.dupes

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
    async def next_batch(self) -> AsyncGenerator[dict, None]:
        """
        Asynchronously retrieve metadata for the next batch of files.

        :return: dict of MergeFile objects that were added
        """
        # yield batch

    async def _loop(self) -> None:
        """Repeatedly call next_batch() until all files are retrieved."""
        # connect to source
        await self.connect()
        # loop over batches
        async for _ in self.next_batch():
            pass # do nothing

    def run(self) -> MergeSet:
        """
        Retrieve metadata for all files.

        :return: MergeSet of all files
        """
        logger.debug("Retrieving logical file metadata from MetaCat")
        # run the loop
        try:
            asyncio.run(self._loop())
        except ValueError as err:
            logger.error("%s", err)
            return MergeSet()

        # log any missing or duplicated files
        io_utils.log_dict("Missing metadata for {n} file{s}:", self.missing)
        io_utils.log_dict("Found {n} duplicate file{s}:", self.dupes)

        return self.files

class MergeFile:
    """A generic data file with metadata"""
    def __init__(self, data: dict):
        self._did = data['namespace'] + ':' + data['name']
        self.fid = data['fid']
        self.size = data['size']
        self.checksums = data['checksums']
        if len(self.checksums) == 0:
            logger.warning("No checksums for %s", self)
        elif 'adler32' not in self.checksums:
            logger.warning("No adler32 checksum for %s", self)
        self.metadata = data['metadata']
        self.parents = data['parents']

    @property
    def did(self) -> str:
        """The file DID (namespace:name)"""
        return self._did

    @property
    def namespace(self) -> str:
        """The file namespace"""
        return self.did.split(':', 1)[0]

    @property
    def name(self) -> str:
        """The file name"""
        return self.did.split(':', 1)[1]

    @property
    def format(self):
        """The file format (core.file_format)"""
        return self.metadata['core.file_format']

    def __eq__(self, other) -> bool:
        return self.did == str(other)

    def __lt__(self, other) -> bool:
        return self.did < other.did

    def __hash__(self) -> int:
        return hash(self.did)

    def __str__(self) -> str:
        return self.did

    def get_fields(self, fields: list) -> tuple:
        """
        Get the namespace and specified metadata values from the file

        :param fields: list of metadata fields to extract
        :return: tuple of values for each field
        """
        values = [self.namespace] + [self.metadata.get(field, "") for field in fields]
        return tuple(values)

class MergeSet(collections.UserDict):
    """Class to keep track of a set of files for merging"""
    def __init__(self, files: list[MergeFile] = None, config: dict = None):
        super().__init__()

        if config is None:
            config = io_utils.read_config()
        self.allow_duplicates = config['validation']['allow_duplicates']
        self.checked_fields = config['validation']['metadata_fields']
        self.field_values = None

        if files:
            self.add_files(files)

    def add_file(self, file: MergeFile | dict) -> MergeFile:
        """
        Add a file to the set

        :param file: A MergeFile object or a dictionary with file metadata
        :return: True if the file was added, False if it was a duplicate
        """
        if isinstance(file, dict):
            file = MergeFile(file)
        did = file.did

        # Check if the file is already in the set
        if did in self.data:
            if not self.allow_duplicates:
                raise ValueError(f"Duplicate file {did} found in input list!")
            self.data[did].count += 1
            logger.debug("Duped file %s", did)
            return None

        # Check if the file metadata is consistent
        vals = file.get_fields(self.checked_fields)
        if not self.field_values:
            # First file, so set the field values
            self.field_values = vals
        elif self.field_values != vals:
            # Check against the first file
            errs = [f"Found inconsistent metadata for file {did}:"]
            if vals[0] != self.field_values[0]:
                errs.append(f"\n  namespace: '{vals[0]}' != '{self.field_values[0]}'")
            for i, field in enumerate(self.checked_fields, start=1):
                if vals[i] != self.field_values[i]:
                    errs.append(f"\n  {field}: '{vals[i]}' != '{self.field_values[i]}'")
            msg = "".join(errs)
            logger.error(msg)
            raise ValueError(msg)

        # Actualy add the file
        self.data[did] = file
        self.data[did].count = 1
        logger.debug("Added file %s", did)
        return file

    def add_files(self, files: Iterable) -> dict:
        """
        Add a collection of files to the set

        :param files: collection of MergeFile objects or dictionaries with file metadata
        :return: dict of MergeFile objects that were added
        """
        new_files = {}
        for file in files:
            new_file = self.add_file(file)
            if new_file is not None:
                new_files[new_file.did] = new_file
        logger.debug("Added %d unique files", len(new_files))
        return new_files

    @property
    def dupes(self) -> dict:
        """Return counts of duplicate file DIDs"""
        return {did:(file.count-1) for did, file in self.data.items() if file.count > 1}

    @property
    def files(self) -> list[MergeFile]:
        """Return the list of files"""
        return sorted(self.data.values())

    def __iter__(self):
        """Iterate over the files"""
        return iter(self.files)

    @property
    def hash(self) -> str:
        """Get a hash from the list of files"""
        concat = '/'.join(sorted(self.data.keys()))
        return hashlib.sha256(concat.encode('utf-8')).hexdigest()

    @property
    def size(self) -> int:
        """Get the total size of the files"""
        return sum(file.size for file in self.data.values())

    def check_consistency(self, fields: list) -> bool:
        """
        Check that the files have consistent namespaces and selected metadata fields

        :param fields: list of metadata fields to check
        :return: True if all files have matching metadata, False otherwise
        """
        logger.debug("Checking metadata consistency")

        if len(self.data) < 2:
            return True

        counts = collections.defaultdict(int)
        for file in self.data.values():
            counts[file.get_fields(fields)] += 1

        if len(counts) == 1:
            return True

        mode = max(counts, key=counts.get)
        n_errs = len(self.data) - counts[mode]
        s_errs = "s" if n_errs != 1 else ""
        errs = [f"Found {n_errs} file{s_errs} with inconsistent metadata:"]

        for file in self.files:
            values = file.get_fields(fields)
            if values != mode:
                errs.append(f"\n  {file.did}")
                if values[0] != mode[0]:
                    errs.append(f"\n    namespace: '{values[0]}' != '{mode[0]}'")
                for i, key in enumerate(fields, start=1):
                    if values[i] != mode[i]:
                        errs.append(f"\n    {key}: '{values[i]}' != '{mode[i]}'")

        logger.error("".join(errs))
        return False


def check_remote_path(path: str, timeout: float = 5) -> bool:
    """Check if a remote path is accessible via xrootd"""
    components = path.split('/', 3)
    url = '/'.join(components[0:3])
    path = '/'+components[3]
    cmd = ['xrdfs', url, 'ls', '-l', path]
    try:
        ret = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.debug("Timeout accessing %s%s", url, path)
        return False
    if ret.returncode == 51:
        logger.debug("Invalid xrootd server %s", url)
        return False
    if ret.returncode == 54:
        logger.debug("No such file %s%s", url, path)
        return False
    if ret.returncode != 0:
        logger.debug("Failed to access %s%s\n  %s", url, path, ret.stderr.strip().split(' ', 1)[1])
        return False
    return True
