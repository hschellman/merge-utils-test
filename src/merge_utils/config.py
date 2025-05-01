"""Module for configuration settings."""

import logging

from merge_utils import io_utils

DEFAULT_CONFIG = "defaults.yaml"

validation: dict = {}
sites: dict = {}
merging: dict = {}

logger = logging.getLogger(__name__)

def load(file: str = None) -> None:
    """
    Load the specified configuration file.
    Missing keys will be filled in with the defaults in DEFAULT_CONFIG.
    
    :param file: Configuration file name or path.
    :return: None
    """
    cfg = io_utils.read_config_file(DEFAULT_CONFIG)
    if file:
        cfg.update(io_utils.read_config_file(file))
        logger.info("Loaded configuration file %s", file)
    else:
        logger.info("Loaded default configuration file %s", DEFAULT_CONFIG)

    global validation, sites, merging # pylint: disable=global-statement

    validation = cfg.get("validation", {})
    sites = cfg.get("sites", {})
    merging = cfg.get("merging", {})
