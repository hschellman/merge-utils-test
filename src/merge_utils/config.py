"""Module for configuration settings."""

import logging

from merge_utils import io_utils

DEFAULT_CONFIG = ["validation.yaml", "defaults.yaml"]

# Configuration dictionaries
inputs: dict = {}
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
        if key not in old_dict:
            # If the key does not exist in the old dictionary, add it
            old_dict[key] = val
            continue
        old_val = old_dict.get(key, None)
        if isinstance(old_val, dict):
            # If both are dictionaries, recursively update
            if isinstance(val, dict):
                recursive_update(old_dict[key], val)
        elif isinstance(old_val, list):
            # If the old value is a list, extend it with the new value
            if isinstance(val, list):
                old_dict[key].extend(val)
            elif val is not None:
                old_dict[key].append(val)
        else:
            old_dict[key] = val

def update(cfg: dict) -> None:
    """
    Update the global configuration with values from the provided dictionary.
    
    :param cfg: Dictionary containing new configuration values.
    :return: None
    """
    recursive_update(inputs, cfg.get("inputs", {}))
    recursive_update(validation, cfg.get("validation", {}))
    recursive_update(sites, cfg.get("sites", {}))
    recursive_update(merging, cfg.get("merging", {}))
    recursive_update(output, cfg.get("output", {}))

def load(files: list = None) -> None:
    """
    Load the specified configuration files.
    Missing keys will be filled in with the defaults in DEFAULT_CONFIG.
    
    :param files: List of configuration files.
    :return: None
    """
    # Add the default configuration file to the beginning of the list
    if not files:
        files = DEFAULT_CONFIG
    elif isinstance(files, str):
        files = DEFAULT_CONFIG + [files]
    else:
        files = DEFAULT_CONFIG + files

    for file in files:
        cfg = io_utils.read_config_file(file)
        logger.info("Loaded configuration file %s", file)
        update(cfg)
