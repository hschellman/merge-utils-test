"""Utility functions for interacting with the Rucio web API."""
from __future__ import annotations

import collections
import logging
import subprocess
#from dataclasses import dataclass

from rucio.client.replicaclient import ReplicaClient
from rucio.client.rseclient import RSEClient

from merge_set import MergeFile, MergeSet

logger = logging.getLogger(__name__)

LOCAL_PING_THRESHOLD = 5

def check_status(path: str) -> bool:
    """Check whether a file is on disk or tape"""
    cmd = ['gfal-xattr', path, 'user.status']
    ret = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if 'ONLINE' in ret.stdout:
        return True
    return False

class Site:
    """Class to store information about a site"""
    def __init__(self, valid: bool):
        self.valid = valid
        self.disk = set()
        self.tape = set()

class Sites(collections.UserDict):
    """Class to keep track of a set of sites"""
    def __init__(self):
        super().__init__()
        self.disk = set()
        self.tape = set()

        # get the list of sites from Rucio
        rse_client = RSEClient()
        for rse in rse_client.list_rses():
            valid = True
            if rse['deleted'] or not rse['availability_read'] or rse['staging_area']:
                valid = False
            self[rse['rse']] = Site(valid)

    def ping(self, pfn: str) -> float:
        """Get the ping time to a site in ms"""
        url = pfn.split(':', 2)[1][2:] # extract host from 'protocol://host:port/path'
        cmd = ['ping', '-c', '1', url]
        ret = subprocess.run(cmd, capture_output=True, check=False)
        if ret.returncode != 0:
            ping = float('inf')
        else:
            ping = float(ret.stdout.split(b'/')[-2]) # average ping time
        logger.debug("Pinged %s, t = %d ms", url, ping)
        return ping

    def get_paths(self, did: str, replicas: dict) -> dict:
        """Get the physical paths for a file"""
        paths = {}

        for pfn, info in replicas['pfns'].items():
            rse = info['rse']
            if rse not in self:
                logger.warning("RSE %s not found in Rucio", rse)
                continue

            if not self[rse].valid:
                logger.warning("RSE %s is not valid", rse)
                continue

            paths[rse] = pfn
            if info['type'] == 'DISK' or check_status(pfn):
                self[rse].disk.add(did)
            else:
                self[rse].tape.add(did)

        return paths

    def cleanup(self) -> None:
        """Cleanup the sites dictionary"""
        rse_client = RSEClient()
        # Remove sites with no files
        self.data = {k: v for k, v in self.items() if len(v.disk) > 0 or len(v.tape) > 0}

        # Count how many files we found
        msg = [""]
        for rse, site in sorted(self.items(), key=lambda x: len(x[1].disk), reverse=True):
            self.disk |= site.disk
            self.tape |= site.tape
            msg.append(f"\n  {rse}: {len(site.disk)} ({len(site.tape)}) files")

            attr = rse_client.list_rse_attributes(rse)
            site.justin = attr['site']
        
        n_files = len(self.disk)
        n_tape = len(self.tape - self.disk)
        n_sites = len(self.items())
        s_files = "s" if n_files != 1 else ""
        s_sites = "s" if n_sites != 1 else ""
        msg[0] = f"Found {n_files} ({n_tape}) file{s_files} from {n_sites} site{s_sites}:"
        logger.info("".join(msg))


def check_consistency(file: MergeFile, rucio: dict) -> bool:
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

def find_physial_files(files : MergeSet) -> MergeSet:
    """Get the best physical locations for a list of logical files"""
    found_files = MergeSet()
    bad_files = []
    inacessible_files = []

    sites = Sites()
    replica_client = ReplicaClient()
    for replicas in replica_client.list_replicas(files.rucio_list(), ignore_availability=False):
        did = replicas['scope'] + ':' + replicas['name']
        file = files[did]
        file.has_rucio = True

        if not check_consistency(file, replicas):
            bad_files.append(did)
            continue

        file.paths = sites.get_paths(did, replicas)
        if len(file.paths) == 0:
            inacessible_files.append(did)
            #logger.error("No valid replicas found for %s", did)
            continue

        found_files.add(file)

    sites.cleanup()

    missing_files = [file.did for file in files if not file.has_rucio]
    log_bad_files(missing_files, "No Rucio entry for {count} {files}:")
    log_bad_files(inacessible_files, "No valid replicas for {count} {files}:")
    log_bad_files(bad_files, "Inconsistent data for {count} {files}:")

    return found_files
