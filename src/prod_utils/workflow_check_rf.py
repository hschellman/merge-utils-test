'''Check the status of individual workflows for a given task.

For each workflow id in a range, counts the parent files and events across
the confirmed merged output and checks that the number of parent (input)
files matches the expected count (batch / chunk).
'''

import os
import sys

from metacat.webapi import MetaCatClient

from get_tasks_rf import get_tasks

CHUNK = 100

USAGE = ("Usage: python -m workflow_check_rf "
         "<TASK> <start_workflow_id> <end_workflow_id>")


def resolve_task(tasks):
    '''Pick the task from argv, exiting with a helpful message if invalid.'''
    if len(sys.argv) < 4:
        print(USAGE)
        sys.exit(1)

    task = sys.argv[1]
    if task not in tasks:
        print(f"Task {task} not found")
        print("Available tasks:", ', '.join(tasks))
        sys.exit(1)
    return task


def summarize_workflow(files):
    '''Sum parent and event counts over the queried merged files.

    Returns (parent_count, event_count, file_count, skip).
    '''
    parent_count = 0
    event_count = 0
    file_count = 0
    skip = 0
    for file in files:
        file_count += 1
        metadata = file["metadata"]
        event_count += metadata["core.event_count"]
        parent_count += len(file["parents"])
        skip = metadata["merge.skip"]
    return parent_count, event_count, file_count, skip


def main():
    tasklist = os.path.join(os.getenv("MERGE_UTILS_DIR"),
                            os.getenv("CAMPAIGN_DIR"),
                            os.getenv("CAMPAIGN") + "_jobs.csv")
    tasks = get_tasks(tasklist)

    task = resolve_task(tasks)
    info = tasks[task]

    nfiles = int(info['NFILES'])
    print("nfiles", nfiles)

    batch = int(info.get("BATCH", 2000))
    expected_parents = batch // CHUNK

    mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])

    start = int(sys.argv[2])
    end = int(sys.argv[3])
    for workflow in range(start, end + 1):
        query = (f"files where merge.tag={task} and "
                 f"dune.output_status=confirmed and "
                 f"dune.workflow['workflow_id']={workflow}")
        files = mc_client.query(query=query, with_metadata=True,
                                with_provenance=True)

        parent_count, event_count, file_count, skip = summarize_workflow(files)

        if parent_count != expected_parents:
            print(f"ERROR: final number of input files {parent_count} "
                  f"is not = the input {expected_parents} "
                  f"for workflow {workflow} skip {skip}")
        else:
            print(f"This pass1 {task} workflow {workflow} with skip {skip} "
                  f"is complete {parent_count} {file_count} "
                  f"({event_count} events)")


if __name__ == '__main__':
    main()
