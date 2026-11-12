'''Check that a pass1 merge tag is complete.

Counts the parent files and events across the confirmed merged output for a
task and compares the parent count against the expected NFILES.
'''

# This code was refactored by Heidi Schellman using Claude Opus 4.8 on 2026-11-12

import os
import sys

from metacat.webapi import MetaCatClient

from get_tasks_rf import get_tasks


def resolve_task(tasks):
    '''Pick the task from argv, exiting with a helpful message if invalid.'''
    task = sys.argv[1] if len(sys.argv) >= 2 else "HELP"
    if task not in tasks:
        print(f"Task {task} not found")
        print("Available tasks:", ', '.join(tasks))
        sys.exit(1)
    return task


def summarize_files(files):
    '''Sum parent and event counts over the queried merged files.

    Returns (parent_count, event_count, file_count).
    '''
    parent_count = 0
    event_count = 0
    file_count = 0
    for file in files:
        file_count += 1
        event_count += file["metadata"]["core.event_count"]
        parent_count += len(file["parents"])
    return parent_count, event_count, file_count


def main():
    campaign_dir = os.getenv("CAMPAIGN_DIR")
    campaign = os.getenv("CAMPAIGN")
    tasklist = os.path.join(campaign_dir, campaign + "_jobs.csv")
    tasks = get_tasks(tasklist)

    task = resolve_task(tasks)
    info = tasks[task]

    nfiles = int(info['NFILES'])
    print("nfiles", nfiles)

    mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])
    query = (f"files where merge.tag={task} and "
             f"dune.output_status=confirmed and namespace={info['NAMESPACE']}")
    files = mc_client.query(query=query, with_metadata=True, with_provenance=True)

    parent_count, event_count, file_count = summarize_files(files)

    print("pass1 this tag had ", parent_count, "parents and ", event_count,
          "events, spread across", file_count, "pass1 files")

    if parent_count != nfiles:
        print(parent_count, nfiles)
        print(f"ERROR: final number of files {parent_count} "
              f"is not = the input {nfiles}")
    else:
        print("This pass1 is complete", task)


if __name__ == '__main__':
    main()
