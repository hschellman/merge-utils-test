''' Script to create a list of unprocessed files for a given tag.

Usage:
    python make_safe_query_rf.py <TAG>

Builds a "safe" metacat query that selects the input files for a tag minus the
parents of files already confirmed for that tag, i.e. the files still to be
processed.
'''

# This code was refactored by Heidi Schellman using Claude Opus 4.8 on 2026-11-12

import os
import sys

from metacat.webapi import MetaCatClient

from get_tasks_rf import get_tasks


def make_safe_query(tasks, tag):
    '''Return a metacat query selecting the not-yet-processed input files for a tag.'''
    campaign = tasks[tag]['CAMPAIGN']
    namespace = tasks[tag]['NAMESPACE']
    input_dataset = tasks[tag]['DATASET']
    fcl = tasks[tag]['FCL']

    children = (f"files where merge.tag={tag} and dune.output_status=confirmed "
                f"and dune.campaign={campaign} and namespace={namespace} "
                f"and dune.config_file={fcl} ordered")
    parents = f"parents ({children})"
    newquery = f"files from {input_dataset} - ({parents})"
    print(newquery)
    return newquery


def query_count(mc_client, query):
    '''Return the file count for a summary query, tolerant of metacat return shape.'''
    result = mc_client.query(query=query, summary="count")
    # Depending on the metacat version this is a dict or an iterable holding one dict
    if not isinstance(result, dict):
        result, = result
    return result['count']


def main():
    tasklist = os.path.join(os.getenv("CAMPAIGN_DIR"),
                            os.getenv("CAMPAIGN") + "_jobs.csv")
    tasks = get_tasks(tasklist)

    if len(sys.argv) < 2:
        print("Usage: python make_safe_query_rf.py <TAG>")
        print("Available tasks:", ', '.join(tasks.keys()))
        sys.exit(1)

    task = sys.argv[1]
    if task not in tasks:
        print(f"Task {task} not found")
        print("Available tasks:", ', '.join(tasks.keys()))
        sys.exit(1)

    mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])

    dataset = tasks[task]['DATASET']
    initial = query_count(mc_client, f"files from {dataset}")

    safe = make_safe_query(tasks, task)
    final = query_count(mc_client, safe)

    print(f"Initial files in {dataset}: {initial}")
    print(f"Final files after safe query: {final}")


if __name__ == "__main__":
    main()
