"""JobScheduler classes"""

import logging
import os
import json
import tarfile
import subprocess
import collections
from abc import ABC, abstractmethod

from merge_utils import io_utils
from merge_utils.retriever import FileRetriever

logger = logging.getLogger(__name__)

class JobScheduler(ABC):
    """Base class for scheduling a merge job"""

    def __init__(self, source: FileRetriever):
        """
        Initialize the JobScheduler with a source of files to merge.
        
        :param source: FileRetriever object to provide input files
        """
        self.source = source
        self.dir = os.path.join(io_utils.pkg_dir(), "tmp", io_utils.get_timestamp())
        self.tier1 = []
        self.tier2 = []

    def get_chunks(self) -> None:
        """Run the FileRetriever and get the list of chunks."""
        self.source.run()
        for chunk in self.source.output_chunks():
            json_dict = chunk.json
            if chunk.chunks: # if the chunk has sub-chunks it is tier 2
                json_name = os.path.join(self.dir, f"tier2_{len(self.tier2):>06}.json")
                self.tier2.append(json_name)
            else: # if the chunk has no sub-chunks it is tier 1
                json_name = os.path.join(self.dir, f"tier1_{len(self.tier1):>06}.json")
                self.tier1.append(json_name)
            with open(json_name, 'w', encoding="utf-8") as fjson:
                fjson.write(json.dumps(json_dict, indent=2))

    @abstractmethod
    def run(self) -> None:
        """Run the job scheduler."""
        raise NotImplementedError("Subclasses must implement this method")

class JustinScheduler():
    """Job scheduler for JustIN merge jobs"""

    def __init__(self, source: FileRetriever):
        """
        Initialize the JustinScheduler with a source of files to merge.
        
        :param source: FileRetriever object to provide input files
        """
        self.source = source
        self.dir = os.path.join(io_utils.pkg_dir(), "tmp", io_utils.get_timestamp())
        self.tier0 = collections.defaultdict(list)
        self.tier1 = collections.defaultdict(list)
        self.cvmfs_dir = None

    def json_name(self, tier: int, site: str = None) -> str:
        """
        Get the name of the next JSON config file for a given tier and site.
        
        :param tier: Tier number (0 or 1)
        :param site: Optional site name
        :return: Name of the JSON file
        """
        tier = int(tier)
        if tier == 0:
            site_jobs = self.tier0[site]
        elif tier == 1:
            site_jobs = self.tier1[site]
        else:
            raise ValueError("Tier must be 0 or 1")

        idx = len(site_jobs) + 1
        if site:
            name = f"tier{tier}_{site}_{idx:>06}.json"
        else:
            name = f"tier{tier}_{idx:>06}.json"
        name = os.path.join(self.dir, name)
        site_jobs.append(name)
        return name

    def get_chunks(self) -> None:
        """Run the FileRetriever and get the list of chunks."""
        os.makedirs(self.dir)
        self.source.run()
        for chunk in self.source.output_chunks():
            json_dict = chunk.json
            site = chunk.site
            name = self.json_name(chunk.chunks > 0, site)
            with open(name, 'w', encoding="utf-8") as fjson:
                fjson.write(json.dumps(json_dict, indent=2))

    def upload_cfg(self) -> None:
        """
        Make a tarball of the configuration files and upload them to cvmfs
        
        :return: Path to the uploaded configuration directory
        """
        cfg = os.path.join(self.dir, "config.tar")
        with tarfile.open(cfg,"w") as tar:
            for _, files in collections.ChainMap(self.tier0, self.tier1).items():
                for file in files:
                    tar.add(file, os.path.basename(file))
            tar.add(os.path.join(io_utils.src_dir(), "do_merge.py"), "do_merge.py")

        proc = subprocess.run(['justin-cvmfs-upload', cfg], capture_output=True, check=False)
        if proc.returncode != 0:
            logger.error("Failed to upload configuration files: %s", proc.stderr.decode('utf-8'))
            raise RuntimeError("Failed to upload configuration files")
        self.cvmfs_dir = proc.stdout.decode('utf-8').strip()
        logger.info("Uploaded configuration files to %s", self.cvmfs_dir)

    def get_cmd(self, tier: int, site: str = None) -> str:
        """
        Get the command to run for a given tier and site.
        
        :param tier: Tier number (0 or 1)
        :param site: Optional site name
        :return: Command string
        """
        tier = int(tier)
        if tier == 0:
            site_jobs = self.tier0[site]
        elif tier == 1:
            site_jobs = self.tier1[site]
        else:
            raise ValueError("Tier must be 0 or 1")

        if not site_jobs:
            raise ValueError(f"No jobs found for tier {tier} and site {site}")

        cmd = [
            'justin', 'simple-workflow',
            '--monte-carlo', str(len(site_jobs)),
            '--jobscript', os.path.join(io_utils.src_dir(), "merge.jobscript"),
            '--env', f'MERGE_CONFIG="tier{tier}_{site}"',
            '--env', f'CONFIG_DIR="{self.cvmfs_dir}"',
            '--env', 'OUT_DIR="/nashome/e/emuldoon/scratch"',
            '--site', site
        ]
        return cmd

    def run(self) -> None:
        """
        Run the JustIN job scheduler.
        
        :return: None
        """
        self.get_chunks()
        if not self.tier0:
            logger.warning("No files to merge")
            return
        self.upload_cfg()

        with open(os.path.join(self.dir, "tier0.sh"), 'w', encoding="utf-8") as f:
            f.write("#!/bin/bash\n")
            f.write("# This script will submit the JustIN jobs for tier 0\n")
            for site in self.tier0:
                cmd = self.get_cmd(0, site)
                f.write(f"{' '.join(cmd)}\n")

        if self.tier1:
            with open(os.path.join(self.dir, "tier1.sh"), 'w', encoding="utf-8") as f:
                f.write("#!/bin/bash\n")
                f.write("# This script will submit the JustIN jobs for tier 1\n")
                for site in self.tier1:
                    cmd = self.get_cmd(1, site)
                    f.write(f"{' '.join(cmd)}\n")

        logger.info("JustIN job scripts written to %s", self.dir)
        #subprocess.run(cmd, check=True)
