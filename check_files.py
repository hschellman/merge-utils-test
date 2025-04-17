import os
import subprocess

def check_status(path):
    """Check whether a file is on disk or tape"""
    if path.startswith("root://"):
        # remote site
        cmd = ['gfal-xattr', path, 'user.status']
        ret = subprocess.run(cmd, capture_output=True, text=True, check=False)
        status = ret.stdout.strip()

        # special case for FNAL DCACHE
        if status=='UNKNOWN' and path.startswith("root://fndca1.fnal.gov:1094/pnfs/fnal.gov/usr"):
            local_path = path.replace("root://fndca1.fnal.gov:1094/pnfs/fnal.gov/usr", "/pnfs")
            print(local_path)
            if not os.path.exists(local_path):
                return status
            directory, filename = os.path.split(local_path)
            stat_file=f"{directory}/.(get)({filename})(locality)"
            with open(stat_file, encoding="utf-8") as stats:
                status = stats.readline().strip()
    else:
        # local site
        if not os.path.exists(path):
            return 'NONEXISTENT'

        # special case for FNAL DCACHE
        path = os.path.realpath(path)
        directory, filename = os.path.split(path)
        stat_file=f"{directory}/.(get)({filename})(locality)"
        if not os.path.exists(stat_file):
            # normal file not in DCACHE?
            return 'ONLINE'

        with open(stat_file, encoding="utf-8") as stats:
            status = stats.readline().strip()

    # status can be 'ONLINE AND NEARLINE', just return one or the other
    if 'ONLINE' in status:
        return 'ONLINE'
    if 'NEARLINE' in status:
        return 'NEARLINE'
    return status

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: check_files.py <file>")
        sys.exit(1)
    for file in sys.argv[1:]:
        print(check_status(file))
