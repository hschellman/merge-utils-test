"""Set up logging"""

import logging.config
import json
import pathlib

def setup() -> None:
    """Configure logging from a JSON file"""
    config_file = pathlib.Path("logs/config.json")
    with open(config_file, encoding="utf-8") as f:
        config = json.load(f)
    logging.config.dictConfig(config)

def override_level(level: str) -> None:
    """Override the logging level"""
    logger = logging.getLogger()
    for handler in logger.handlers:
        if handler.get_name() == "console":
            handler.setLevel(level)
