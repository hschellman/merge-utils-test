"""Utility functions for merging metadata for multiple files."""

import logging
import collections

from merge_utils import config, io_utils

logger = logging.getLogger(__name__)

def fix(name: str, metadata: dict) -> dict:
    """
    Fix the metadata dictionary.

    :param name: name of the file (for logging)
    :param metadata: metadata dictionary
    :return: fixed metadata dictionary
    """
    # Fix misspelled keys
    for bad_key, good_key in config.validation['fixes']['keys'].items():
        if bad_key in metadata:
            logger.warning("File %s replacing metadata key %s with %s", name, bad_key, good_key)
            metadata[good_key] = metadata.pop(bad_key)
    # Fix missing keys
    for key, value in config.validation['fixes']['missing'].items():
        if key not in metadata:
            logger.warning("File %s metadata key %s is missing, setting to %s", name, key, value)
            metadata[key] = value
    # Fix misspelled values
    for key in config.validation['fixes']:
        if key in ['keys', 'missing'] or key not in metadata:
            continue
        value = metadata[key]
        if value in config.validation['fixes'][key]:
            new_value = config.validation['fixes'][key][value]
            logger.warning("File %s replacing %s value %s with %s", name, key, value, new_value)
            metadata[key] = new_value
    return metadata

def validate(name: str, metadata: dict) -> dict:
    """
    Validate the metadata dictionary.

    :param name: name of the file (for logging)
    :param metadata: metadata dictionary
    :raises ValueError: if the metadata is invalid
    """
    # Fix metadata
    metadata = fix(name, metadata)
    # Always require run_type and data_tier
    for key in ["core.run_type", "core.data_tier"]:
        if key not in metadata:
            raise ValueError(f"File {name} metadata missing required key: {key}")
    # Get optional keys
    optionals = config.validation['optional']['all']
    optionals += config.validation['optional'].get(metadata["core.data_tier"], {})
    if metadata["core.run_type"] != "mc":
        optionals += config.validation['optional']['mc']
    # Check required keys
    for key, values in config.validation['required'].items():
        if key in optionals:
            continue
        if key not in metadata:
            raise ValueError(f"File {name} metadata missing required key: {key}")
        value = metadata[key]
        if value not in values:
            raise ValueError(f"File {name} invalid value for {key}: {value}")
    # Check value types
    for key, expected_type in config.validation['types'].items():
        if key in optionals:
            continue
        if key not in metadata:
            raise ValueError(f"File {name} metadata missing required key: {key}")
        value = metadata[key]
        type_name = type(value).__name__
        if (type_name == expected_type) or (expected_type == 'float' and type_name == 'int'):
            continue
        raise ValueError(f"File {name} invalid type for {key}: {value} (expected {expected_type})")
    return metadata

class MergeMetaMin:
    """Merge metadata by taking the minimum value."""
    warn = False

    def __init__(self):
        self.value = float('inf')

    def add(self, value):
        """Add a new value to the metadata."""
        self.value = min(self.value, value)

    @property
    def valid(self):
        """Check if the value is valid."""
        return self.value != float('inf')

class MergeMetaMax:
    """Merge metadata by taking the maximum value."""
    warn = False

    def __init__(self):
        self.value = -float('inf')

    def add(self, value):
        """Add a new value to the metadata."""
        self.value = max(self.value, value)

    @property
    def valid(self):
        """Check if the value is valid."""
        return self.value != -float('inf')

class MergeMetaSum:
    """Merge metadata by adding the values."""
    warn = False

    def __init__(self):
        self.value = 0

    def add(self, value):
        """Add a new value to the metadata."""
        self.value += value

    @property
    def valid(self):
        """Check if the value is valid."""
        return self.value != 0

class MergeMetaUnion:
    """Merge metadata by taking the union."""
    warn = False

    def __init__(self):
        self._value = set()

    def add(self, value):
        """Add a new value to the metadata."""
        self._value.update(value)

    @property
    def value(self):
        """Get the merged value."""
        return list(self._value)

    @property
    def valid(self):
        """Check if the value is valid."""
        return len(self._value) > 0

class MergeMetaUnique:
    """Merge metadata by taking the unique values."""
    def __init__(self, value=None):
        self.value = value
        self._valid = True
        self.warn = False

    def add(self, value):
        """Add a new value to the metadata."""
        if self.value is None:
            self.value = value
        elif self.value != value:
            self._valid = False
            self.warn = True

    @property
    def valid(self):
        """Check if the value is valid."""
        return self._valid and self.value is not None

class MergeMetaAll:
    """Merge metadata by taking the set of values."""
    warn = False

    def __init__(self):
        self._value = set()

    def add(self, value):
        """Add a new value to the metadata."""
        self._value.update(value)

    @property
    def value(self):
        """Get the merged value."""
        if len(self._value) == 1:
            return next(iter(self._value))
        return list(self._value)

    @property
    def valid(self):
        """Check if the value is valid."""
        return len(self._value) > 0

class MergeMetaOverride:
    """Merge metadata by overriding the value."""
    warn = False

    def __init__(self, value=None):
        self.value = value

    def add(self, value):
        """Add a new value to the metadata."""

    @property
    def valid(self):
        """Check if the value is valid."""
        return self.value is not None

MERGE_META_CLASSES = {
    'unique': MergeMetaUnique,
    'all': MergeMetaAll,
    'min': MergeMetaMin,
    'max': MergeMetaMax,
    'sum': MergeMetaSum,
    'union': MergeMetaUnion,
    #'skip': MergeMetaOverride,
}

def merged_keys(files: dict, warn: bool = False) -> dict:
    """
    Merge metadata from multiple files into a single dictionary.

    :param files: set of files to merge
    :param warn: whether to warn about inconsistent metadata
    :return: merged metadata
    """
    metadata = collections.defaultdict(
        MERGE_META_CLASSES[config.merging['metadata']['default']]
    )
    for key, mode in config.merging['metadata'].items():
        if key in ['default', 'overrides']:
            continue
        if mode in MERGE_META_CLASSES:
            metadata[key] = MERGE_META_CLASSES[mode]()
        else:
            metadata[key] = MergeMetaOverride()
    for key, value in config.merging['metadata']['overrides'].items():
        metadata[key] = MergeMetaOverride(value)
    for file in files.values():
        for key, value in file.metadata.items():
            metadata[key].add(value)

    if warn:
        io_utils.log_list("Omitting {n} inconsistent metadata key{s}:",
            [k for k, v in metadata.items() if v.warn]
        )
    metadata = {k: v.value for k, v in metadata.items() if v.valid}
    return validate("output", metadata)

def parents(files: dict) -> list[str]:
    """
    Retrieve all the parents from a set of files.

    :param files: set of files to merge
    :return: set of parents
    """
    if not config.output['grandparents']:
        return list(files.keys())
    grandparents = set()
    for file in files.values():
        grandparents.update(file.parents)
    return list(grandparents)

class MergeMetaNameDict(collections.UserDict):
    """Class to inject metadata into a name template."""

    def __init__(self, value: str = None):
        """
        Initialize the NameMetaFormatter with a value.

        :param value: value to insert
        """
        super().__init__()
        self._str = value or ""

    def __str__(self):
        return self._str

    def __repr__(self):
        if not self._str:
            return f"{self.data}"
        if not self.data:
            return f"'{self._str}'"
        return f"'{self._str}'{self.data}"

    def __getitem__(self, name):
        name_list = name.split('.', 1)
        if name_list[0] not in self.data:
            self.data[name_list[0]] = MergeMetaNameDict(name_list[0])
        if len(name_list) == 1:
            return self.data[name_list[0]]
        return self.data[name_list[0]][name_list[1]]

    def __setitem__(self, name, value):
        self[name]._str = value

    def __getattr__(self, name):
        return self[name]

def make_name(metadata: dict) -> str:
    """
    Create a name for the merged file based on the metadata.

    :param metadata: metadata dictionary
    :return: merged file name
    """
    inserts = MergeMetaNameDict()
    for key, value in metadata.items():
        if not isinstance(value, str):
            value = str(value)
        value = value.split('.', 1)[0]
        value = config.output['abbreviations'].get(key, {}).get(value, value)
        inserts[key] = value
    inserts['timestamp'] = io_utils.get_timestamp()

    name = config.output['name'].format_map(inserts)
    ext = config.merging['methods'][config.merging['method']]['ext']
    return name + ext
