'''Script to check the status of merge workflows for a given task.

Usage:
    python workflow_check_rf.py <TASK> <start_workflow_id> <end_workflow_id>

Reads task definitions from $MERGE_UTILS_DIR/$CAMPAIGN_DIR/$CAMPAIGN_jobs.csv
and, for each workflow id in the requested range, checks that the number of
input (parent) files matches the expected batch/chunk size.
'''

# This code was refactored by Heidi Schellman using Claude Opus 4.8 on 2026-11-12

import os
import sys

from metacat.webapi import MetaCatClient

from get_tasks import get_tasks

CHUNK = 100
DEFAULT_BATCH = 2000


def get_tasklist_path():
    '''Return the path to the <campaign>_jobs.csv task list from the environment.'''
    return os.path.join(os.getenv("MERGE_UTILS_DIR"),
                        os.getenv("CAMPAIGN_DIR"),
                        os.getenv("CAMPAIGN") + "_jobs.csv")


def check_workflow(mc_client, task, workflow):
    '''Query metacat for one workflow and return (nparents, event_count, filecount, skip).'''
    query = ("files where merge.tag=%s and dune.output_status=confirmed "
             "and dune.workflow['workflow_id']=%d" % (task, workflow))
    files = mc_client.query(query=query, with_metadata=True, with_provenance=True)

    event_count = 0
    nparents = 0
    filecount = 0
    skip = 0
    for file in files:
        filecount += 1
        metadata = file["metadata"]
        event_count += metadata["core.event_count"]
        nparents += len(file["parents"])
        skip = metadata["merge.skip"]
    return nparents, event_count, filecount, skip


def main():
    tasks = get_tasks(get_tasklist_path())

    if len(sys.argv) < 4:
        print("Usage: python workflow_check_rf.py <TASK> <start_workflow_id> <end_workflow_id>")
        sys.exit(1)

    task = sys.argv[1]
    start_workflow = int(sys.argv[2])
    end_workflow = int(sys.argv[3])

    if task not in tasks:
        print(f"Task {task} not found")
        print("Available tasks:", ', '.join(tasks.keys()))
        sys.exit(1)

    nfiles = int(tasks[task]['NFILES'])
    print("nfiles", nfiles)

    # batch was referenced but never assigned in the original script; restored
    # from the task's BATCH field per the commented-out intent (default 2000).
    batch = int(tasks[task].get("BATCH", DEFAULT_BATCH))

    mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])

    for workflow in range(start_workflow, end_workflow + 1):
        nparents, event_count, filecount, skip = check_workflow(mc_client, task, workflow)
        if nparents != batch / CHUNK:
            print("ERROR: final number of input files %d is not = the input %d "
                  "for workflow %d skip %d" % (nparents, batch, workflow, skip))
        else:
            print("This pass1 %s workflow %d with skip %d is complete %d %d"
                  % (task, workflow, skip, nparents, filecount))


if __name__ == "__main__":
    main()
