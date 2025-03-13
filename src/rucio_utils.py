"""Utility functions for interacting with the Rucio web API."""

import collections
import logging
import subprocess
from dataclasses import dataclass

from rucio.client.replicaclient import ReplicaClient

from src.file_utils import DataFile, DataSet

logger = logging.getLogger(__name__)

LOCAL_PING_THRESHOLD = 5

@dataclass
class Site:
    """Class to store information about a site"""
    ping: float
    count: int = 1
    best: int = 0

class Sites(collections.UserDict):
    """Class to keep track of a set of sites"""
    def __init__(self, prefer_site: str = None, local_only = False):
        super().__init__()
        self.prefer_site = prefer_site
        self.local_only = local_only

    def ping(self, rse: str, pfn: str) -> float:
        """Check the ping time to a site in ms, recording the number of times it is used"""
        if rse in self.data:
            self.data[rse].count += 1
            return self.data[rse].ping

        url = pfn.split(':', 2)[1][2:] # extract host from 'protocol://host:port/path'
        cmd = ['ping', '-c', '1', url]
        ret = subprocess.run(cmd, capture_output=True, check=False)
        if ret.returncode != 0:
            ping = float('inf')
        else:
            ping = float(ret.stdout.split(b'/')[-2]) # average ping time
        logger.debug("Pinged %s (%s), t = %d ms", rse, url, ping)

        self.data[rse] = Site(ping)
        return ping

    def get_paths(self, replicas: dict) -> list[str]:
        """Get the best paths for a file from a list of replicas"""
        paths = []
        for pfn, info in replicas['pfns'].items():
            rse = info['rse']
            ping = self.ping(rse, pfn)
            if ping == float('inf'):
                continue

            priority = info['priority']
            if ping < LOCAL_PING_THRESHOLD:
                priority = ping - LOCAL_PING_THRESHOLD # prioritize local sites
            if self.prefer_site and rse == self.prefer_site:
                priority = -100

            if self.local_only:
                if self.prefer_site and rse != self.prefer_site:
                    continue
                if not self.prefer_site and ping >= LOCAL_PING_THRESHOLD:
                    continue

            #check whether the file is on disk or tape?
            #if info['type'] == 'DISK':
            #elif info['type'] == 'TAPE':

            paths.append([priority, rse, pfn])

        paths.sort()
        self.data[paths[0][1]].best += 1
        return [path[2] for path in paths]

    def log_counts(self) -> None:
        """Log the number of files found from each site"""
        msg = [""]
        n_files = 0
        n_sites = len(self)
        for rse, site in sorted(self.items(), key=lambda x: x[1].best, reverse=True):
            n_files += site.best
            msg.append(f"\n  {rse}: {site.best} ({site.count}) files")
        s_files = "s" if n_files != 1 else ""
        s_sites = "s" if n_sites != 1 else ""
        msg[0] = f"Found {n_files} file{s_files} from {n_sites} site{s_sites}:"
        logger.info("".join(msg))


def check_consistency(file: DataFile, rucio: dict) -> bool:
    """Ensure consistency between MetaCat and Rucio"""
    # Check the file size
    if file.size != rucio['bytes']:
        logger.error("Size mismatch for %s: %d != %d", file.did, file.size, rucio['bytes'])
        return False
    # Adler32 checksum is the default
    if 'adler32' in file.checksums and 'adler32' in rucio:
        logger.debug("Checking adler32 checksum for %s", file.did)
        csum1 = file.checksums['adler32']
        csum2 = rucio['adler32']
        match = csum1 == csum2
        if not match:
            logger.error("Adler32 checksum mismatch for %s: %s != %s", file.did, csum1, csum2)
        return match
    logger.warning("No adler32 checksum for %s", file.did)
    # Check other checksums
    for algo, csum1 in file.checksums.items():
        if algo in rucio:
            logger.debug("Checking %s checksum for %s", algo, file.did)
            csum2 = rucio[algo]
            match = csum1 == csum2
            if not match:
                logger.error("%s checksum mismatch for %s: %s != %s", algo, file.did, csum1, csum2)
            return match
        logger.debug("Rucio missing %s checksum for %s", algo, file.did)
    logger.error("No matching checksum for %s", file.did)
    return False

def log_bad_files(files: dict, msg: str) -> int:
    """Log a message for files without valid replicas"""
    total = len(files)
    if total == 0:
        return 0
    if total == 1:
        msg = [msg.format(count=1, files="file")]
    else:
        msg = [msg.format(count=total, files="files")]
    msg += [f"\n  {file}" for file in sorted(files)]
    logger.warning("".join(msg))
    return total

def find_physial_files(files : DataSet, prefer_site: str = None, local_only = False) -> DataSet:
    """Get the best physical locations for a list of logical files"""
    found_files = DataSet()
    bad_files = []
    inacessible_files = []

    sites = Sites(prefer_site, local_only)
    replica_client = ReplicaClient()
    for replicas in replica_client.list_replicas(files.rucio_list(), ignore_availability=False):
        did = replicas['scope'] + ':' + replicas['name']
        file = files[did]
        file.has_rucio = True

        if not check_consistency(file, replicas):
            bad_files.append(did)
            continue

        file.paths = sites.get_paths(replicas)
        if len(file.paths) == 0:
            inacessible_files.append(did)
            #logger.error("No valid replicas found for %s", did)
            continue

        found_files.add(file)

    sites.log_counts()
    missing_files = [file.did for file in files if not file.has_rucio]
    log_bad_files(missing_files, "No Rucio entry for {count} {files}:")
    log_bad_files(inacessible_files, "No valid replicas for {count} {files}:")
    log_bad_files(bad_files, "Inconsistent data for {count} {files}:")

    return found_files
