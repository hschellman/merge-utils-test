"""Utility functions for merging metadata for multiple files."""

import logging
import collections
import typing

from merge_utils import config, io_utils

logger = logging.getLogger(__name__)

# Metadata keys that don't make sense to merge
IGNORED_KEYS = [
    "core.start_time",
    "core.end_time",
    "core.data_tier",
    "Offline.options",
    "Offline.machine",
]

# Metadata keys that require special handling
SPECIAL_KEYS = [
    "core.first_event_number",
    "core.last_event_number",
    "core.event_count",
    "core.events"
    "core.runs",
    "core.runs_subruns",
]

def special_keys(files: dict) -> dict:
    """
    Retrieve all the special keys from a set of files.

    :param files: set of files to merge
    :return: metadata dictionary
    """
    first_event = 1e12
    last_event = -1e12
    event_count = 0
    runs = set()
    subruns = set()
    events = set()

    for file in files.values():
        first_event = min(first_event, file.metadata.get("core.first_event_number", 1e12))
        last_event = max(last_event, file.metadata.get("core.last_event_number", -1e12))
        event_count += file.metadata.get("core.event_count", 0)
        events.update(file.metadata.get("core.events", []))
        runs.update(file.metadata.get("core.runs", []))
        subruns.update(file.metadata.get("core.runs_subruns", []))

    return {
        "core.first_event_number": first_event,
        "core.last_event_number": last_event,
        "core.event_count": event_count,
        "core.events": list(events),
        "core.runs": list(runs),
        "core.runs_subruns": list(subruns),
    }

def normal_keys(files: dict) -> dict:
    """
    Retrieve all the normal keys from a set of files.

    :param files: set of files to merge
    :return: metadata dictionary
    """
    meta = collections.defaultdict(set)
    unhashable_keys = set()
    overrides = config.output['metadata_overrides'].keys()
    for file in files.values():
        for key, value in file.metadata.items():
            # skip metadata with special rules
            if key in SPECIAL_KEYS or key in IGNORED_KEYS or key in overrides:
                continue
            if not isinstance(value, typing.Hashable):
                unhashable_keys.add(key)
                continue
            meta[key].add(value)
    io_utils.log_list("Ignoring {n} un-hashable key{s}", unhashable_keys, logging.DEBUG)
    return meta

def shared_keys(files: dict) -> dict:
    """
    Retrieve all the shared keys from a set of files.

    :param files: set of files to merge
    :return: metadata dictionary
    """
    meta = normal_keys(files)
    return {key: list(value)[0] for key, value in meta.items() if len(value) == 1}

def merged_keys(files: dict) -> dict:
    """
    Merge metadata from multiple files into a single dictionary.

    :param files: set of files to merge
    :return: merged metadata
    """
    meta = shared_keys(files)
    meta.update(special_keys(files))
    meta.update(config.output['metadata_overrides'])
    return meta

def get_parents(files: dict) -> set:
    """
    Retrieve all the parents from a set of files.

    :param files: set of files to merge
    :return: set of parents
    """
    if not config.output['grandparents']:
        return set(files.keys())
    parents = set()
    for file in files.values():
        parents.update(file.parents)
    return parents

def make_name(metadata: dict) -> str:
    """
    Create a name for the merged file based on the metadata.

    :param metadata: metadata dictionary
    :return: merged file name
    """
    name = config.output['name']
    for key, value in metadata.items():
        if not isinstance(value, str):
            value = str(value)
        value = value.split('.', 1)[0]
        if value in config.abbreviations:
            value = config.abbreviations[value]

        name = name.replace(f"{{{key}}}", value)
    name = name.replace("{timestamp}", io_utils.get_timestamp())
    #return name.format_map(inserts)
    return name
