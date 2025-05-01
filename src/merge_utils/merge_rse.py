"""Keep track of RSEs where files are stored"""

import os
import collections
import logging
import subprocess
import csv
import asyncio
from typing import Iterable

import requests

from rucio.client.rseclient import RSEClient

from . import io_utils

logger = logging.getLogger(__name__)

SITES_STORAGES_URL = "https://justin-ui-pro.dune.hep.ac.uk/api/info/sites_storages.csv"
LOCAL_PING_THRESHOLD = 5

def check_path(path: str) -> str:
    """
    Check the status of a file path.

    :param path: file path to check.  Can be a local file or a remote URL.
    :return: status of the file ('ONLINE', 'NEARLINE', 'NONEXISTENT', or 'UNKNOWN')
    """
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
    logger.warning("File %s status is %s", path, status)
    return status

class MergeRSE:
    """Class to store information about an RSE"""
    def __init__(self, valid: bool, nearline_dist: float = 0):
        self.valid = valid
        self.disk = {}
        self.tape = {}
        self.distances = {}
        self.nearline_dist = nearline_dist
        self.ping = float('inf')

    @property
    def pfns(self) -> dict:
        """Get the set of PFNs for this RSE"""
        return collections.ChainMap(self.disk, self.tape)

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

class MergeRSEs(collections.UserDict):
    """Class to keep track of a set of RSEs"""
    def __init__(self, config: dict = None):
        super().__init__()
        if config is None:
            config = io_utils.read_config()
        self.sites = config['sites']['allowed_sites']
        self.max_distance = config['sites']['max_distance']
        self.nearline_distances = config['sites']['nearline_distance']

        self.disk = set()
        self.tape = set()

    async def connect(self) -> None:
        """Download the RSE list from Rucio and determine their distances"""
        # Get the list of RSEs from Rucio
        rse_client = RSEClient()
        rses = await asyncio.to_thread(rse_client.list_rses)
        for rse in rses:
            valid = (
                rse['availability_read']
                and not rse['staging_area']
                and not rse['deleted']
            )
            if rse['rse'] in self.nearline_distances:
                nearline_dist = self.nearline_distances[rse['rse']]
            else:
                nearline_dist = self.nearline_distances['default']
            self[rse['rse']] = MergeRSE(valid, nearline_dist)

        # Get site distances from justIN web API
        fields = ['site', 'rse', 'dist', 'site_enabled', 'rse_read', 'rse_write']
        res = await asyncio.to_thread(requests.get, SITES_STORAGES_URL, verify=False, timeout=10)
        text = res.iter_lines(decode_unicode=True)
        reader = csv.DictReader(text, fields)
        for row in reader:
            if not row['site_enabled'] or not row['rse_read']:
                continue
            site = row['site']
            rse = row['rse']
            if site not in self.sites or rse not in self or not self[rse].valid:
                continue
            self[rse].distances[site] = 100*float(row['dist'])

        n_accessible = sum(1 for rse in self.values() if rse.valid
                           and rse.nearest_site()[1] <= self.max_distance)
        logger.info("Found %d RSEs accessible from %d sites", n_accessible, len(self.sites))

    async def add_pfn(self, did: str, pfn: str, info: dict) -> float:
        """Add a file PFN to the corresponding RSE
        
        :param did: file DID
        :param pfn: physical file name
        :param info: dictionary with RSE information
        :return: distance from the RSE to the nearest site
        """
        # Check if the RSE is valid
        rse = info['rse']
        if rse not in self or not self[rse].valid:
            logger.warning("RSE %s does not exist?", rse)
            return float('inf')
        if not self[rse].valid:
            logger.warning("RSE %s is invalid", rse)
            return float('inf')
        _, dist = self[rse].nearest_site()
        if dist > self.max_distance:
            logger.warning("RSE %s is too far from merging sites (%s > %s)",
                           rse, dist, self.max_distance)
            return dist

        # Check file status
        if info['type'] == 'DISK':
            status = 'ONLINE'
        else:
            status = await asyncio.to_thread(check_path, pfn)

        # Actually add the file to the RSE
        if status == 'ONLINE':
            self.disk.add(did)
            self.data[rse].disk[did] = pfn
        elif status == 'NEARLINE':
            dist += self.data[rse].nearline_dist
            if dist > self.max_distance:
                logger.warning("RSE %s (tape) is too far from merging sites (%s > %s)",
                               rse, dist, self.max_distance)
            else:
                self.tape.add(did)
                self.data[rse].tape[did] = pfn
        else:
            # File is not accessible
            return float('inf')

        return dist

    async def add_replicas(self, did: str, replicas: dict) -> int:
        """Add a set of file replicas to the RSEs
        
        :param did: file DID
        :param replicas: Rucio replica dictionary
        :return: number of PFNs added
        """
        tasks = [self.add_pfn(did, pfn, info) for pfn, info in replicas['pfns'].items()]
        results = await asyncio.gather(*tasks)
        best_dist = min(results)
        if best_dist > self.max_distance:
            if best_dist == float('inf'):
                logger.error("Could not retrieve file %s from Rucio", did)
                raise ValueError("File not found")
            logger.error("File %s is too far from merging sites (%s > %s)",
                         did, best_dist, self.max_distance)
            raise ValueError("File exceeds max distance")
        return sum(1 for dist in results if dist <= self.max_distance)

    def cleanup(self) -> None:
        """Remove RSEs with no files"""
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
        msg[0] = (f"Found {n_files} ONLINE ({n_tape} NEARLINE) file{s_files} "
                  f"from {n_rses} RSE{s_rses}:")
        logger.info("".join(msg))

    def site_pfns(self, site: str, dids: Iterable) -> dict:
        """Get the shortest distance pfns for a given site"""
        pfns = {}
        for did in dids:
            pfns[did] = (None, float('inf'))

        for name, rse in self.items():
            distance = rse.distance(site)
            if distance > self.max_distance:
                logger.info("RSE %s is too far away from site %s (%s > %s)",
                            name, site, distance, self.max_distance)
                continue
            for did, pfn in rse.disk.items():
                if did not in dids:
                    continue
                if distance < pfns[did][1]:
                    pfns[did] = (pfn, distance)

            distance += rse.nearline_dist
            if distance > self.max_distance:
                logger.info("RSE %s (tape) is too far away from site %s (%s > %s)",
                            name, site, distance, self.max_distance)
                continue
            for did, pfn in rse.tape.items():
                if did not in dids:
                    continue
                if distance < pfns[did][1]:
                    pfns[did] = (pfn, distance)
        return pfns

    def best_pfns(self, dids: Iterable = None) -> dict:
        """Determine the best RSE for each file"""

        if dids is None:
            dids = self.disk | self.tape

        site_pfns = {}
        best_site = None
        best_distance = float('inf')
        for site in self.sites:
            site_pfns[site] = self.site_pfns(site, dids)
            total_distance = sum(t[1] for t in site_pfns[site].values())
            if total_distance < best_distance:
                best_distance = total_distance
                best_site = site

        if best_site:
            logger.info("Site %s has the shortest distance to all files", best_site)
            return {best_site: site_pfns[best_site]}

        logger.info("No site found with access to all files")
        print(dids)
        print(site_pfns)
        site_pfns[None] = {}
        for did in dids:
            best_site = None
            best_distance = float('inf')
            for site in self.sites:
                if site_pfns[site][did][1] < best_distance:
                    best_site = site
                    best_distance = site_pfns[site][did][1]
            #best_site, best_distance = min(
                #((site, pfns[did][1]) for (site, pfns) in site_pfns.items()), key=lambda x: x[1])
            if best_distance > self.max_distance:
                best_site = None
                site_pfns[None][did] = (None, float('inf'))
            # remove all but the best pfn
            for site in self.sites:
                if site == best_site:
                    continue
                del site_pfns[site][did]

        if io_utils.log_list("Excessive distance for {n} file{s}:", site_pfns[None].keys()):
            logger.error("Consider adjusting site distance limits!")
            return {}

        return {site: pfns for (site, pfns) in site_pfns.items() if len(pfns) > 0}
