'''
Script to create pass1 merge commands for a given task.

Reads a task definition from the campaign CSV, checks MetaCat for files that
already exist (so a --retry is added when needed), and writes the merge
command(s) to <task>.sh.
'''

# This code was refactored by Heidi Schellman using Claude Opus 4.8 on 2026-11-12


import os
import sys
from datetime import datetime, timezone

from metacat.webapi import MetaCatClient

from get_tasks_rf import get_tasks

RETRY_FLAG = "--retry"
DEFAULT_BATCH = 2000


def count_files(mc_client, query):
    '''Return the number of files matching a MetaCat count query.'''
    result = mc_client.query(query=query, summary="count")
    # Some MetaCat versions return a one-element sequence, others a bare dict.
    try:
        check, = result
    except (TypeError, ValueError):
        check = result
    return check["count"]


def build_merge_command(config_path, campaign, task, dataset, local, retry,
                        timestamp, skip=0, limit=None):
    '''Build a single `merge` shell command line.'''
    parts = ["merge"]
    if retry:
        parts.append(retry)
    if local:
        parts.append(local)
    parts += ["-vv", "-c", config_path]
    if limit is not None:
        parts += [f"--skip={skip}", f"--limit={limit}"]
    parts += [f'--campaign="{campaign}"', f'--tag="{task}"', "dataset", dataset]
    logfile = f"{task}_{timestamp}_{skip}.log"
    return " ".join(parts) + f" > {logfile} 2>&1 "


def resolve_task(tasks):
    '''Pick the task from argv, exiting with a helpful message if invalid.'''
    available = ", ".join(tasks)
    if len(sys.argv) < 2:
        print("Need to specify a task")
        print("Available tasks:", available)
        sys.exit(1)

    task = sys.argv[1]
    if task not in tasks:
        print(f"Task {task} not found")
        print("Available tasks:", available)
        sys.exit(1)
    return task


def check_version(info):
    '''Ensure DUNE_VERSION is set and matches the task's required version.'''
    version = os.getenv("DUNE_VERSION", "unknown")
    dunesw = info.get("DUNESW", "unknown")
    if version == "unknown":
        sys.exit("Quitting: DUNE_VERSION environment variable not set, you need "
                 "to set DUNE_VERSION and then set up larsoft and merging again.")
    if version != dunesw:
        print(f"Quitting: DUNE_VERSION environment variable ({version}) does not "
              f"match the required DUNESW version for the task ({dunesw})")
        sys.exit("Unfortunately this means you have to start a new session, set "
                 "DUNE_VERSION and then rerun setup_fnal.sh")


def main():
    '''Make a set of merge commands for a particular tag.'''
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    campaign_env = os.getenv("CAMPAIGN")
    if not campaign_env:
        sys.exit("Please set CAMPAIGN environment variable using the "
                 "setup_campaign.sh script")

    campaign_dir = os.getenv("CAMPAIGN_DIR")
    tasks = get_tasks(os.path.join(campaign_dir, campaign_env + "_jobs.csv"))

    task = resolve_task(tasks)
    info = tasks[task]

    retry = RETRY_FLAG if (len(sys.argv) > 2 and sys.argv[2].lower() == "retry") else ""
    local = "-l" if "TEST" in task else ""

    batch = int(info.get("BATCH", DEFAULT_BATCH))
    print("batch", batch)

    check_version(info)

    config = info["CONFIG"]
    campaign = info["CAMPAIGN"]
    dataset = info["DATASET"]
    namespace = info["NAMESPACE"]
    nfiles = int(info["NFILES"])
    print("nfiles", nfiles)

    mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])

    base_query = (f"files where merge.tag={task} and "
                  f"dune.output_status=confirmed and namespace={namespace}")

    # Global duplicate check: turn on --retry if any files already exist.
    print("query", base_query)
    dupcount = count_files(mc_client, base_query)
    print("dupcount", dupcount)
    if dupcount > 0:
        retry = RETRY_FLAG
        print(f"Found {dupcount} existing files for task {task}, "
              f"will use {retry} option")

    with open(f"{task}.sh", "w") as f:
        if nfiles < batch:
            print("less than batch")
            command = build_merge_command(f"{campaign_dir}/{config}", campaign,
                                          task, dataset, local, retry, timestamp)
            print(command)
            f.write(command + "\n")
        else:
            baseline_retry = retry
            for skip in range(0, nfiles, batch):
                step = batch
                print("next", skip, step)
                chunk_query = (f"{base_query} and "
                               f"merge.skip={skip} and merge.limit={step}")
                # A chunk with existing files forces --retry; otherwise fall
                # back to the baseline decided by the global check / argv.
                chunk_retry = (RETRY_FLAG if count_files(mc_client, chunk_query) > 0
                               else baseline_retry)
                command = build_merge_command(f"{campaign_dir}/{config}", campaign,
                                              task, dataset, local, chunk_retry,
                                              timestamp, skip=skip, limit=step)
                print(command)
                f.write(command + "\n")


if __name__ == '__main__':
    main()
