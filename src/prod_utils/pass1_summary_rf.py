'''Make a summary of pass1 for every tag in a campaign.

For each task in the campaign job list, queries the confirmed merged output,
tallies parent files, events and average file size, checks the parent count
against the expected NFILES, and writes a per-task summary CSV.
'''

import os

from metacat.webapi import MetaCatClient

from get_tasks_rf import get_tasks


def build_query(task, info):
    '''Build the MetaCat query for a task's confirmed merged files.'''
    return (f"files where merge.tag={task} and "
            f"namespace={info['NAMESPACE']} and "
            f"dune.output_status=confirmed and "
            f"dune.campaign={info['CAMPAIGN']} and "
            f"merge.dataset='{info['DATASET']}' and "
            f"merge.cfg='{info['FCL']}'")


def summarize_files(files):
    '''Sum parent, event and size counts over the queried merged files.

    Returns (parent_count, event_count, file_count, total_size_gb).
    '''
    parent_count = 0
    event_count = 0
    file_count = 0
    total_size = 0.0
    for file in files:
        file_count += 1
        event_count += file["metadata"]["core.event_count"]
        parent_count += len(file["parents"])
        total_size += file['size'] / 1_000_000_000
    return parent_count, event_count, file_count, total_size


def main():
    print("Summary of pass1 for all tags")

    tasklist = os.path.join(os.environ["CAMPAIGN_DIR"],
                            os.environ["CAMPAIGN"] + "_jobs.csv")
    tasks = get_tasks(tasklist)
    summary_path = tasklist.replace("jobs.csv", "Pass1Summary.csv")

    mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])

    with open(summary_path, "w", encoding='utf-8') as out:
        out.write("TASK, NFILES, CHECK, NFILES_OUTPUT, QUERY\n")

        for task, info in tasks.items():
            print(f"-------------------------- {task} --------------------------")
            nfiles = int(info['NFILES'])
            print("nfiles", nfiles)

            query = build_query(task, info)
            print(query)
            files = mc_client.query(query=query, with_metadata=True,
                                    with_provenance=True)

            parent_count, event_count, file_count, total_size = summarize_files(files)
            avesize = total_size / file_count if file_count > 0 else 0

            print("Average file size for pass1 files: %8.3f GB" % avesize)
            print("this query had ", parent_count, "parents and ", event_count,
                  "events, spread across ", file_count, " pass1 files")

            if parent_count != nfiles:
                print("ERROR: final number of files %d from %s is not = the input %d"
                      % (parent_count, task, nfiles))
            else:
                print("This task is complete", task)

            out.write("%s, %d, %d, %d, \"%s\" \n"
                      % (task, nfiles, parent_count, file_count, query))


if __name__ == '__main__':
    main()
