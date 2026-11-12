'''Read task definitions from a campaign CSV file.'''

import csv
import os
import sys


def get_tasks(tasklist):
    '''Return a dict mapping each task's TAG to its CSV row (a dict of columns).

    Exits the program with a message if the file is missing, has no TAG
    column, or contains duplicate tags.
    '''
    if not os.path.exists(tasklist):
        sys.exit(f"Task list file {tasklist} not found")

    tasks = {}
    with open(tasklist, encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        if reader.fieldnames is None or "TAG" not in reader.fieldnames:
            sys.exit(f"Task list file {tasklist} has no 'TAG' column")
        for row in reader:
            tag = row["TAG"]
            if tag in tasks:
                sys.exit(f"Duplicate task tag '{tag}' in {tasklist}")
            tasks[tag] = row
    return tasks
