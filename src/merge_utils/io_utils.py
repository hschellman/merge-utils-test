"""Utilites for reading and writing files and other I/O operations"""

import os
import sys
import logging
import logging.config
import json
import pathlib

# tomllib was added to the standard library in Python 3.10, need tomli for DUNE
try:
    import tomllib # type: ignore
except ImportError:
    import tomli as tomllib

import yaml

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = "config/defaults.toml"

def get_inputs(file_path: str, args: list[str]) -> list[str]:
    """
    Get a list of inputs from various sources, including
    1. A file containing a list of entries
    2. entries passed as command line arguments
    3. entries piped in from standard input
    """

    if len(args) > 0:
        logger.debug("Found %d entires from command line", len(args))
    inputs = args

    if file_path is not None:
        with open(file_path, encoding="utf-8") as f:
            entries = f.readlines()
        logger.debug("Found %d entries in file %s", len(entries), file_path)
        inputs.extend([x.strip() for x in entries])

    if not sys.stdin.isatty():
        entries = sys.stdin.readlines()
        logger.debug("Found %d entries from standard input", len(entries))
        inputs.extend([x.strip() for x in entries])

    return inputs

def read_config(file_path: str = None) -> dict:
    """
    Read a configuration file in JSON, TOML, or YAML format
    Any missing keys will be filled in with the defaults in DEFAULT_CONFIG
    """
    with open(DEFAULT_CONFIG, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    if file_path is None:
        return config

    if not os.path.exists(file_path):
        logger.error("Could not open %s", file_path)
        return config

    suffix = pathlib.Path(file_path).suffix
    if suffix in [".json"]:
        logger.debug("Reading JSON file %s", file_path)
        with open(file_path, encoding="utf-8") as f:
            config.update(json.load(f))
    elif suffix in [".toml"]:
        logger.debug("Reading TOML file %s", file_path)
        with open(file_path, mode="rb") as f:
            config.update(tomllib.load(f))
    elif suffix in [".yaml", ".yml"]:
        logger.debug("Reading YAML file %s", file_path)
        with open(file_path, encoding="utf-8") as f:
            config.update(yaml.safe_load(f))
    else:
        logger.error("Unknown file type: %s", suffix)
    return config

def setup_log(name: str) -> None:
    """Configure logging"""
    config_file = pathlib.Path("config/logging.json")
    with open(config_file, encoding="utf-8") as f:
        config = json.load(f)
    logging.config.dictConfig(config)

    # If we're appending to an existing log file, add a newline before the new log
    log_file = config['handlers']['file']['filename']
    if os.path.exists(log_file):
        with open(config['handlers']['file']['filename'], 'a', encoding="utf-8") as logfile:
            logfile.write("\n")

    logger.info("Starting script %s", os.path.basename(name))

def set_log_level(level: str) -> None:
    """Override the logging level for the console"""
    for handler in logging.getLogger().handlers:
        if handler.get_name() == "console":
            handler.setLevel(level)
