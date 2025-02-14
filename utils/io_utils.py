"""Utilites for reading and writing files and other I/O operations"""

import sys
import logging

logger = logging.getLogger(__name__)

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
