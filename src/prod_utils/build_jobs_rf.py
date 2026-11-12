'''Script to build a job list with file counts and sizes from a base job list CSV file.

Reads $CAMPAIGN_DIR/$CAMPAIGN.csv and writes:
  - $CAMPAIGN_jobs.csv       one row per sub-campaign, with file count and size
  - $CAMPAIGN_checklist.csv  one row per workflow (batch of files) for tracking
'''

# This code was refactored by Heidi Schellman using Claude Opus 4.8 on 2026-11-12

import csv
import math
import os
import sys

from metacat.webapi import MetaCatClient

DEBUG = True

JOBS_FIELDS = ['TAG', 'DUNESW', 'NFILES', 'SIZE_GB', 'BATCH',
               'FCL', 'CONFIG', 'CAMPAIGN', 'NAMESPACE', 'DATASET']

# Blank tracking columns added to every checklist row, with their initial values
CHECKLIST_DEFAULTS = {
    'WORKFLOW iD': "",
    'PASS': "pass1",
    'TIMESTAMP': "",
    '# of jobs': "",
    'status': "",
    '#event/job': "",
    '#files': "",
    'success fraction': 0.0,
    'total # of events': 0,
    'volume (GB)': 0.0,
    'comments': "?",
}

CHECKLIST_FIELDS = ['NAMESPACE', 'TAG', 'SKIP', 'TIMESTAMP', 'PASS', 'WORKFLOW iD',
                    '# of jobs', 'status', '#event/job', '#files', 'success fraction',
                    'total # of events', 'volume (GB)',
                    'comments', 'DUNESW', 'NFILES', 'SIZE_GB', 'BATCH',
                    'FCL', 'CONFIG', 'CAMPAIGN', 'DATASET']


def get_paths():
    '''Return (joblist, jobs_csv, checklist_csv) paths from the environment.'''
    campaign_dir = os.getenv("CAMPAIGN_DIR")
    campaign = os.getenv("CAMPAIGN")
    if campaign_dir is None or campaign is None:
        sys.exit("Please set CAMPAIGN_DIR and CAMPAIGN environment variables")
    joblist = os.path.join(campaign_dir, campaign + ".csv")
    return (joblist,
            joblist.replace('.csv', '_jobs.csv'),
            joblist.replace('.csv', '_checklist.csv'))


def check_config(config, fcl, campaign):
    '''Exit if the config file is inconsistent with the fcl and campaign in the job list.'''
    with open(config, 'r') as f:
        text = f.read()
    if "lar" in text and fcl not in text:
        sys.exit(f"lar config file {config} does not contain {fcl} - please fix this")
    if campaign not in text:
        sys.exit(f"config file {config} does not contain dune.campaign:{campaign} - please fix this")


def count_files(mc_client, dataset, limit=None):
    '''Return (nfiles, total_size_bytes) for confirmed files in a dataset.'''
    query = f"files from {dataset} where dune.output_status=confirmed"
    if limit is not None:
        query += f" limit {limit}"
    if DEBUG:
        print(query)
    result = mc_client.query(query=query, summary="count")
    # Depending on the metacat version this is a dict or an iterable holding one dict
    if not isinstance(result, dict):
        result, = result
    return result['count'], result['total_size']


def make_flows(job):
    '''Split a job into one checklist row per batch of files.'''
    batch = job['BATCH']
    flows = []
    for flow in range(math.ceil(job['NFILES'] / batch)):
        row = dict(job, SKIP=flow * batch)
        row.update(CHECKLIST_DEFAULTS)
        flows.append(row)
    return flows


def write_csv(path, fieldnames, rows):
    with open(path, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    joblist, jobs_csv, checklist_csv = get_paths()
    print("Using joblist:", joblist)

    mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])

    jobs = []
    flows = []
    with open(joblist, encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        if DEBUG:
            print(reader.fieldnames)
        for row in reader:
            if row['TAG'][0:1] == '#':
                print("Skipping comment line:", row)
                continue
            limit = None
            if "TEST" in row['TAG']:
                limit = 10
            row['BATCH'] = int(row['BATCH'])
            check_config(row['CONFIG'], row['FCL'], row['CAMPAIGN'])
            if DEBUG:
                print(row['TAG'], row['DATASET'])

            nfiles, size = count_files(mc_client, row['DATASET'], limit)
            print("Result for tag", row['TAG'],
                  ": #files=%d total size=%8.3f GB" % (nfiles, size / 1e9))
            row['NFILES'] = nfiles
            row['SIZE_GB'] = round(size / 1e9, 3)

            jobs.append(row)
            flows.extend(make_flows(row))
            print(row)

    write_csv(jobs_csv, JOBS_FIELDS, jobs)
    write_csv(checklist_csv, CHECKLIST_FIELDS, flows)


if __name__ == "__main__":
    main()
