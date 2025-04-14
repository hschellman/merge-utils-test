#!/usr/bin/env python3
"""Actually perform the merging"""

import sys
import json
import subprocess

def merge_hadd(out_file: str, in_files: list) -> None:
    """Merge the input files using hadd"""
    cmd = ['hadd', '-v', '0', '-f', out_file] + in_files
    subprocess.run(cmd, check=True)

def main():
    """Main function for command line execution"""
    with open(sys.argv[1], encoding="utf-8") as f:
        config = json.load(f)
    merge_hadd(config['output'], config['inputs'])

if __name__ == '__main__':
    main()
