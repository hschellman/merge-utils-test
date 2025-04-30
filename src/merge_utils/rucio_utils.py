"""Utility functions for interacting with the Rucio web API."""
from __future__ import annotations

import os
import collections
import logging
import subprocess
import csv
import requests
#from dataclasses import dataclass

from rucio.client.replicaclient import ReplicaClient
from rucio.client.rseclient import RSEClient

from merge_utils.merge_set import MergeFile, MergeSet

logger = logging.getLogger(__name__)

SITES_STORAGES_URL = "https://justin-ui-pro.dune.hep.ac.uk/api/info/sites_storages.csv"
LOCAL_PING_THRESHOLD = 5

def check_status(path: str) -> str:
    """Check whether a file is on disk or tape"""
    if "://" in path:
        # remote site
        cmd = ['gfal-xattr', path, 'user.status']
        ret = subprocess.run(cmd, capture_output=True, text=True, check=False)
        status = ret.stdout.strip()

        # special case for FNAL DCACHE
        fnal_prefix = "root://fndca1.fnal.gov:1094/pnfs/fnal.gov/usr"
        if status=='UNKNOWN' and path.startswith(fnal_prefix):
            local_path = path.replace(fnal_prefix, "/pnfs")
            if not os.path.exists(local_path):
                logger.info("Got UNKNOWN status for %s, assuming NEARLINE", path)
                return 'NEARLINE'
            directory, filename = os.path.split(local_path)
            stat_file=f"{directory}/.(get)({filename})(locality)"
            with open(stat_file, encoding="utf-8") as stats:
                status = stats.readline().strip()
    else:
        # local site
        if not os.path.exists(path):
            logger.warning("File %s not found!", path)
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
    logger.warning("File %s has status %s", path, status)
    return status

class RSE:
    """Class to store information about an RSE"""
    def __init__(self, valid: bool):
        self.valid = valid
        self.disk = {}
        self.tape = {}
        self.distances = {}
        self.ping = float('inf')

    def get_ping(self) -> float:
        """Get the ping time to the RSE in ms"""
        if len(self.disk) > 0:
            pfn = next(iter(self.disk.values()))
        elif len(self.tape) > 0:
            pfn = next(iter(self.tape.values()))
        else:
            return float('inf')
        url = pfn.split(':', 2)[1][2:] # extract host from 'protocol://host:port/path'
        cmd = ['ping', '-c', '1', url]
        ret = subprocess.run(cmd, capture_output=True, check=False)
        if ret.returncode == 0:
            self.ping = float(ret.stdout.split(b'/')[-2]) # average ping time
            logger.debug("Pinged %s, t = %d ms", url, self.ping)
        else:
            logger.warning("Failed to ping %s", url)
        return self.ping

    def distance(self, site: str) -> float:
        """Get the distance to a site"""
        return self.distances.get(site, float('inf'))

    def nearest_site(self) -> tuple:
        """Get the nearest site for this RSE"""
        if len(self.distances) == 0:
            return None, float('inf')
        site = min(self.distances, key=self.distances.get)
        distance = self.distances[site]
        return site, distance

class RSEs(collections.UserDict):
    """Class to keep track of a set of RSEs"""
    def __init__(self):
        super().__init__()
        self.disk = set()
        self.tape = set()

        # get the list of RSEs from Rucio
        rse_client = RSEClient()
        for rse in rse_client.list_rses():
            valid = True
            if rse['deleted'] or not rse['availability_read'] or rse['staging_area']:
                valid = False
            self[rse['rse']] = RSE(valid)

    def add(self, rse: str, status: str, did: str, pfn: str) -> bool:
        """Add a file to the RSE"""
        if status == 'ONLINE':
            self.disk.add(did)
            self.data[rse].disk[did] = pfn
            return True
        if status == 'NEARLINE':
            self.tape.add(did)
            self.data[rse].tape[did] = pfn
            return True
        return False

    def add_replicas(self, did: str, replicas: dict) -> bool:
        """Get the physical paths for a file"""
        added = False

        for pfn, info in replicas['pfns'].items():
            rse = info['rse']
            if rse not in self:
                logger.warning("RSE %s not found in Rucio", rse)
                continue
            if not self[rse].valid:
                logger.warning("RSE %s is not valid", rse)
                continue
            if info['type'] == 'DISK':
                status = 'ONLINE'
            else:
                status = check_status(pfn)
            if not self.add(rse, status, did, pfn):
                continue
            added = True

        return added

    def get_distances(self, sites: list) -> None:
        """Get the distances between sites and storage elements"""
        fields = ['site', 'rse', 'dist', 'site_enabled', 'rse_read', 'rse_write']
        r = requests.get(SITES_STORAGES_URL, verify=False, timeout=10)
        text = r.iter_lines(decode_unicode=True)
        reader = csv.DictReader(text, fields)
        for row in reader:
            if not row['site_enabled'] or not row['rse_read']:
                continue
            if row['site'] not in sites or row['rse'] not in self:
                continue
            self[row['rse']].distances[row['site']] = float(row['dist'])

    def cleanup(self) -> None:
        """Cleanup the RSE dictionary"""
        rse_client = RSEClient()
        # Remove RSEs with no files
        self.data = {k: v for k, v in self.items() if len(v.disk) > 0 or len(v.tape) > 0}

        # Count how many files we found
        msg = [""]
        for name, rse in sorted(self.items(), key=lambda x: len(x[1].disk), reverse=True):
            msg.append(f"\n  {name}: {len(rse.disk)} ({len(rse.tape)}) files")

            attr = rse_client.list_rse_attributes(name)
            rse.site = attr['site']

        n_files = len(self.disk)
        n_tape = len(self.tape - self.disk)
        n_rses = len(self.items())
        s_files = "s" if n_files != 1 else ""
        s_rses = "s" if n_rses != 1 else ""
        msg[0] = f"Found {n_files} ({n_tape}) file{s_files} from {n_rses} RSE{s_rses}:"
        logger.info("".join(msg))

    def site_pfns(self, site: str, max_distance: float, nearline_distance: float) -> dict:
        """Get the shortest distance pfns for a given site"""
        pfns = {}
        for did in self.disk | self.tape:
            pfns[did] = (None, float('inf'))

        for name, rse in self.items():
            distance = rse.distance(site)
            if distance > max_distance:
                logger.info("RSE %s is too far away from site %s (%s > %s)",
                            name, site, distance, max_distance)
                continue
            for did, pfn in rse.disk.items():
                if distance < pfns[did][1]:
                    pfns[did] = (pfn, distance)

            distance += nearline_distance
            if distance > max_distance:
                logger.info("RSE %s (tape) is too far away from site %s (%s > %s)",
                            name, site, distance, max_distance)
                continue
            for did, pfn in rse.tape.items():
                if distance < pfns[did][1]:
                    pfns[did] = (pfn, distance)
        return pfns

    def get_pfns(self, config: dict) -> dict:
        """Determine the best RSE for each file"""
        sites = config['allowed_sites']
        max_distance = config['max_distance']
        nearline_distance = config['nearline_distance']

        self.cleanup()
        self.get_distances(sites)

        site_pfns = {}
        best_site = None
        best_distance = float('inf')
        for site in sites:
            site_pfns[site] = self.site_pfns(site, max_distance, nearline_distance)
            total_distance = sum(t[1] for t in site_pfns[site].values())
            if total_distance < best_distance:
                best_distance = total_distance
                best_site = site

        if best_site:
            logger.info("Site %s has the shortest distance to all files", best_site)
            return {best_site: site_pfns[best_site]}

        logger.info("No site found with access to all files")
        site_pfns[None] = {}
        for did in self.disk | self.tape:
            best_site = None
            best_distance = float('inf')
            for site in sites:
                if site_pfns[site][did][1] < best_distance:
                    best_site = site
                    best_distance = site_pfns[site][did][1]
            #best_site, best_distance = min(
                #((site, pfns[did][1]) for (site, pfns) in site_pfns.items()), key=lambda x: x[1])
            if best_distance > max_distance:
                best_site = None
                site_pfns[None][did] = (None, float('inf'))
            # remove all but the best pfn
            for site in sites:
                if site == best_site:
                    continue
                del site_pfns[site][did]

        return {site: pfns for (site, pfns) in site_pfns.items() if len(pfns) > 0}

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

def find_physical_files(files: MergeSet, config: dict) -> MergeSet:
    """Get the best physical locations for a list of logical files"""
    bad_files = []
    inacessible_files = []

    rses = RSEs()
    replica_client = ReplicaClient()
    for replicas in replica_client.list_replicas(files.rucio, ignore_availability=False):
        did = replicas['scope'] + ':' + replicas['name']
        file = files[did]
        file.has_rucio = True

        if not check_consistency(file, replicas):
            bad_files.append(did)
            continue

        if not rses.add_replicas(did, replicas):
            inacessible_files.append(did)
            continue

    site_pfns = rses.get_pfns(config)
    distant_files = list(site_pfns.get(None, {}).keys())
    missing_files = [file.did for file in files if not file.has_rucio]
    errs = log_bad_files(missing_files, "No Rucio entry for {count} {files}:")
    errs += log_bad_files(inacessible_files, "No valid replicas for {count} {files}:")
    errs += log_bad_files(bad_files, "Inconsistent data for {count} {files}:")
    errs += log_bad_files(distant_files, "Excessive distance for {count} {files}:")

    if errs > 0:
        return {}

    # found all files, add the PFNs to the files
    for pfns in site_pfns.values():
        for did, (pfn, _) in pfns.items():
            files[did].pfn = pfn

    return site_pfns
