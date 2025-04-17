"""Utilities for working with data files"""
from __future__ import annotations
import collections
import logging
import subprocess
import hashlib

logger = logging.getLogger(__name__)

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
        self.has_rucio = False

    @property
    def did(self) -> str:
        """Return the DID of the file"""
        return self._did

    @property
    def namespace(self) -> str:
        """Extract the namespace from the file DID"""
        return self.did.split(':', 1)[0]

    @property
    def name(self) -> str:
        """Extract the name from the file DID"""
        return self.did.split(':', 1)[1]

    @property
    def format(self):
        """Return the file format"""
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
    def __init__(self, files: list[MergeFile] = None):
        super().__init__()
        for file in files or []:
            self.add(file)

    def add(self, file: MergeFile | dict) -> None:
        """
        Add a file to the set

        :param file: A MergeFile object or a dictionary with file metadata
        """
        if isinstance(file, dict):
            file = MergeFile(file)
        did = file.did

        if did not in self.data:
            self.data[did] = file
            self.data[did].count = 1
            logger.debug("Added file %s", did)
        else:
            self.data[did].count += 1
            logger.debug("Duped file %s", did)

    @property
    def dupes(self) -> dict:
        """Return counts of duplicate file DIDs"""
        return {did:(file.count-1) for did, file in self.data.items() if file.count > 1}

    @property
    def rucio(self) -> list[dict]:
        """Return a list of file DIDs in the format expected by Rucio"""
        return [{'scope':file.namespace, 'name':file.name} for file in self.data.values()]

    @property
    def files(self) -> list[MergeFile]:
        """Return the list of files"""
        return sorted(self.data.values())

    def __iter__(self):
        """Iterate over the files"""
        return iter(self.files())

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

        for file in self.files():
            values = file.get_fields(fields)
            if values != mode:
                errs.append(f"\n  {file.did}")
                for i, key in enumerate(fields):
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
