"""Container for a set of files to be merged"""

from __future__ import annotations
import collections
import logging
import subprocess
import hashlib
import math
from typing import Iterable, Generator

from merge_utils import config, meta

logger = logging.getLogger(__name__)

class MergeFile:
    """A generic data file with metadata"""
    def __init__(self, data: dict, path: str = None):
        self._did = data['namespace'] + ':' + data['name']
        self.path = path
        self.fid = data['fid']
        self.size = data['size']
        self.checksums = data['checksums']
        if len(self.checksums) == 0:
            logger.warning("No checksums for %s", self)
        elif 'adler32' not in self.checksums:
            logger.warning("No adler32 checksum for %s", self)
        self.metadata = meta.validate(self._did, data['metadata'])
        self.parents = data.get('parents', [])

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
    def __init__(self, files: list[MergeFile] = None):
        super().__init__()

        self.allow_duplicates = config.validation['allow_duplicates']
        self.consistent_fields = config.validation['consistency']
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
        vals = file.get_fields(self.consistent_fields)
        if not self.field_values:
            # First file, so set the field values
            self.field_values = vals
        elif self.field_values != vals:
            # Check against the first file
            errs = [f"Found inconsistent metadata for file {did}:"]
            if vals[0] != self.field_values[0]:
                errs.append(f"\n  namespace: '{vals[0]}' != '{self.field_values[0]}'")
            for i, field in enumerate(self.consistent_fields, start=1):
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

    def group_count(self) -> list[int]:
        """Group input files by count"""
        target_size = config.merging['target_size']
        total_size = len(self.data)
        if total_size < target_size:
            logger.info("Merging %d inputs into 1 group", len(self.data))
            return [len(self.data)]

        if config.merging['equalize']:
            n_groups = math.ceil(total_size / target_size)
            target_size = total_size / n_groups
            divs = [round(i*target_size) for i in range(1, n_groups)]
        else:
            divs = list(range(target_size, total_size, target_size))
        divs.append(len(self.data))
        logger.info("Merging %d inputs into %d groups of %d files",
                        len(self.data), len(divs), target_size)
        return divs

    def group_size(self) -> list[int]:
        """Group input files by size"""
        target_size = config.merging['target_size'] * 1024**3
        total_size = self.size
        if total_size < target_size:
            logger.info("Merging %d inputs into 1 group", len(self.data))
            return [len(self.data)]

        avg_size = total_size / len(self.data)
        if avg_size > target_size / config.merging['chunk_min']:
            logger.error("%.2f GB input files are too large to merge into %.2f GB groups",
                            avg_size/1024**3, target_size/1024**3)
            return []

        size = 0
        divs = []
        if config.merging['equalize']:
            max_size = target_size
            n_groups = math.ceil(total_size / target_size)
            target_size = total_size / n_groups
            err = 0
            for idx, file in enumerate(self.files):
                # Try to get just above target_size without exceeding max_size
                if size >= target_size - err or size + file.size > max_size:
                    divs.append(idx)
                    err += size - target_size # distribute error across groups
                    size = 0
                size += file.size
        else:
            for idx, file in enumerate(self.files):
                if size + file.size > target_size:
                    divs.append(idx)
                    size = 0
                size += file.size

        divs.append(len(self.data))
        logger.info("Merging %d inputs into %d groups of %.2f GB",
                        len(self.data), len(divs), target_size / 1024**3)
        return divs

    def groups(self) -> Generator[dict, None, None]:
        """Split the files into groups for merging"""
        # Get merged metadata
        merge_meta = meta.merged_keys(self.data, warn = True)

        # Figure out the merge method if not already set
        if config.merging['method'] == 'auto':
            fmt = merge_meta['core.file_format']
            for method, cfg in config.merging['methods'].items():
                if fmt in cfg['file_format']:
                    config.merging['method'] = method
                    logger.info("Using merge method '%s' for file format '%s'", method, fmt)
                    break

        merge_name = meta.make_name(merge_meta)
        merge_hash = self.hash

        # Get the group divisions
        if config.merging['target_mode'] == 'count':
            divs = self.group_count()
        elif config.merging['target_mode'] == 'size':
            divs = self.group_size()
        else:
            logger.error("Unknown target mode: %s", config.merging['target_mode'])
            return
        if len(divs) == 0:
            return

        # Actually output the groups
        group_id = 0
        group = MergeChunk(merge_name, merge_hash)
        if len(divs) > 1:
            group.group_id = group_id
        for i, file in enumerate(self.files):
            group.add(file)
            # Check if we need to yield the group
            if i+1 == divs[group_id]:
                logger.debug("Yielding group %d with %d files", group_id, len(group))
                yield group
                group_id += 1
                group = MergeChunk(merge_name, merge_hash, group=group_id)

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


class MergeChunk(collections.UserDict):
    """Class to keep track of a chunk of files for merging"""
    def __init__(self, name: str, merge_hash: str, group: int = -1):
        super().__init__()
        self._name = name
        self.merge_hash = merge_hash
        self.site = None
        self.group_id = group
        self.chunk_id = -1
        self.chunks = 0

    @property
    def name(self) -> str:
        """The name of the chunk"""
        the_name, ext = self._name.split('.', 1)
        if self.group_id >= 0:
            the_name = f"{the_name}_f{self.group_id}"
        if self.chunk_id >= 0:
            the_name = f"{the_name}_c{self.chunk_id}"
        return f"{the_name}.{ext}"

    @property
    def inputs(self) -> list[str]:
        """Get the list of input files"""
        if self.chunks == 0:
            return [file.path for file in self.data.values()]
        the_name, ext = self.name.split('.', 1)
        return [f"{the_name}_c{idx}.{ext}" for idx in range(self.chunks)]

    @property
    def metadata(self) -> dict:
        """Get the metadata for the chunk"""
        md = meta.merged_keys(self.data)
        md['merge.method'] = config.merging['method']
        md['merge.hash'] = self.merge_hash
        if self.group_id >= 0:
            md['merge.group'] = self.group_id
        if self.chunk_id >= 0:
            md['merge.chunk'] = self.chunk_id
        return md

    @property
    def parents(self) -> list[str]:
        """Get the list of parent dids"""
        return meta.parents(self.data)

    @property
    def json(self) -> dict:
        """Get the chunk metadata as a JSON-compatible dictionary"""
        data = {
            'name': self.name,
            'metadata': self.metadata,
            'parents': self.parents,
            'inputs': self.inputs,
        }
        return data

    def add(self, file: MergeFile) -> None:
        """Add a file to the chunk"""
        self.data[file.did] = file

    def chunk(self) -> MergeChunk:
        """Create a subset of the chunk with the same metadata"""
        chunk = MergeChunk(self.name, self.merge_hash, self.group_id)
        chunk.site = self.site
        chunk.chunk_id = self.chunks
        self.chunks += 1
        return chunk


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
