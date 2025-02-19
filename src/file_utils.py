"""Utilities for working with data files"""

import collections
import logging

logger = logging.getLogger(__name__)

class DataFile:
    """A generic data file with metadata"""
    def __init__(self, data: dict):
        self.name = data['name']
        self.namespace = data['namespace']
        self.size = data['size']
        self.metadata = data['metadata']

    @property
    def did(self):
        """Return the DID (namespace:name) for the file"""
        return self.namespace + ':' + self.name
    
    @property
    def format(self):
        """Return the file format"""
        return self.metadata['core.file_format']

    def __eq__(self, other):
        return self.did == other.did

    def __hash__(self):
        return hash(self.did)

    def __str__(self):
        return self.did

class UniqueFileList(collections.UserList):
    """Class to keep track of unique files"""

    def __init__(self, initlist=None):
        super().__init__(initlist)
        self.counts = {}

    def add(self, file: dict) -> None:
        """Add a file to the list if it is not there already"""
        file = DataFile(file)
        did = file.did

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
