"""Module for configuration settings."""

import logging

from merge_utils import io_utils

DEFAULT_CONFIG = "defaults.yaml"

# Configuration dictionaries
validation: dict = {}
sites: dict = {}
merging: dict = {}
output: dict = {}

initialized: bool = False

logger = logging.getLogger(__name__)

def recursive_update(old_dict: dict, new_dict: dict) -> None:
    """
    Recursively update dictionary d with values from dictionary u.
    
    :param old_dict: Dictionary to be updated.
    :param new_dict: Dictionary with new values.
    :return: None
    """
    for key, val in new_dict.items():
        if isinstance(val, dict) and key in old_dict:
            recursive_update(old_dict[key], val)
        else:
            old_dict[key] = val

def load(file: str = None) -> None:
    """
    Load the specified configuration file.
    Missing keys will be filled in with the defaults in DEFAULT_CONFIG.
    
    :param file: Configuration file name or path.
    :return: None
    """
    global initialized # pylint: disable=global-statement
    if not initialized:
        initialized = True
        load(DEFAULT_CONFIG)
    if not file:
        return
    cfg = io_utils.read_config_file(file)
    logger.info("Loaded configuration file %s", file)

    recursive_update(validation, cfg.get("validation", {}))
    recursive_update(sites, cfg.get("sites", {}))
    recursive_update(merging, cfg.get("merging", {}))
    recursive_update(output, cfg.get("output", {}))
