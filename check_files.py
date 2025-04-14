import os

def check_file(path):
    """Check if a file is staged on FNAL DCACHE"""
    # Convert xRootD path to local path
    if path.startswith("root://fndca1.fnal.gov:1094/"):
        path = path.replace("root://fndca1.fnal.gov:1094/pnfs/fnal.gov/usr", "/pnfs")
    elif path.startswith("root://"):
        print("Attempting to access a file on a remote site")
        return False

    # Check if the file exists
    path = os.path.realpath(path)
    print(path)
    if not os.path.exists(path):
        print("File does not exist")
        return False

    directory, filename = os.path.split(path)
    stat_file=f"{directory}/.(get)({filename})(locality)"
    if not os.path.exists(stat_file):
        print("Normal file not in dCache?")
        return True

    status = ""
    with open(stat_file, encoding="utf-8") as stats:
        status = stats.readline().strip()
    if "ONLINE" in status:
        print("File is staged")
        return True
    if "NEARLINE" in status:
        print("File is nearline")
        return False
    if status == "UNAVAILABLE":
        print("File is unavailable, contact an admin")
        return False
    if status == "LOST":
        print("File is lost!")
        return False
    print(f"Unknown status: {status}")
    return False

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: check_files.py <file>")
        sys.exit(1)
    for file in sys.argv[1:]:
        check_file(file)
