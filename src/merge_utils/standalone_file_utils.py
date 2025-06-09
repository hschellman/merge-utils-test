"""Utility functions for interacting with the MetaCat web API."""

import logging
#import itertools
#import asyncio
import json
import os
from typing import AsyncGenerator

#import metacat.webapi as metacat

from merge_utils import config
from merge_utils.retriever import FileRetriever

logger = logging.getLogger(__name__)

class StandAloneRetriever():
    """Class for access to a list of files with local metadata"""

    def __init__(self, filelist: list = None):
        """
        Initialize the list of files.

        :param filelist: list of file filepaths to find
        """
        super().__init__()

        
        self.filelist = filelist
        self.parents = config.output['grandparents']

        if not self.filelist:
            self.filelist = []

        self.client = None

    def _get_files(self, idx: int, filepaths: list) -> list:
        """
        Asynchronously request file data from MetaCat
        
        :param idx: batch number (for logging)
        :param filepaths: list of filepaths to request
        :return: list of results from the request
        """
        if len(filepaths) == 0:
            return []
        logger.debug("Retrieving files from file pathlist for batch %d", idx)
        dictlist = [{'filepath':filepath} for filepath in filepaths]
        # try:
        #     res = await asyncio.to_thread(self.client.get_files, dictlist,
        #                                   with_metadata = True, with_provenance = self.parents)
        # except (ValueError, metacat.webapi.BadRequestError) as err:
        #     logger.error("%s", err)
        #     return []
        res = []
        lost = []
        for filepath in dictlist:
            if not os.path.exists(filepath):
                logger.warning("file does not exist %s", filepath)
                lost.append(filepath)
                continue
            metafilename = filepath + ".json"
            if not os.path.exists(metafilename):
                logger.warning("metadata file does not exist %s", filepath)
                lost.append(filepath)
                continue
            
            with open(metafilename, 'r',encoding="utf-8") as metafile:
                newdata = json.load(metafile)
            res.append(newdata)

        return list(res)

    # async def connect(self) -> None:
    #     return

    # async def input_batches(self) -> AsyncGenerator[dict, None]:
    #     """
    #     Asynchronously retrieve metadata for the next batch of files.

    #     :return: dict of MergeFile objects that were added
    #     """
    #     return
    #     # # request first batch from filelist
    #     # filepaths = self.filelist[0:self.step]
    #     # task = asyncio.create_task(self._get_files(0, filepaths))
    #     # # loop over batches from filelist
    #     # for idx in range(1, len(self.filelist)//self.step + 1):
    #     #     old_filepaths = filepaths
    #     #     filepaths = self.filelist[idx*self.step:(idx+1)*self.step]
    #     #     res = await task
    #     #     task = asyncio.create_task(self._get_files(idx, filepaths))
    #     #     added = await self.add(res, old_filepaths)
    #     #     logger.debug("yielding file batch %d", idx-1)
    #     #     yield added
    #     # res = await task
    #     # # request first batch from query
    #     # task = asyncio.create_task(self._get_query(0))
    #     # # finish processing last batch from filelist
    #     # if res:
    #     #     added = await self.add(res, filepaths)
    #     #     logger.debug("yielding last file batch")
    #     #     yield added
    #     # # loop over batches from query
    #     # for idx in itertools.count(1):
    #     #     res = await task
    #     #     if len(res) < self.step:
    #     #         break
    #     #     task = asyncio.create_task(self._get_query(idx))
    #     #     added = await self.add(res)
    #     #     logger.debug("yielding query batch %d", idx-1)
    #     #     yield added
    #     # # yield last partial batch from query
    #     # if res:
    #     #     added = await self.add(res)
    #     #     logger.debug("yielding last query batch")
    #     #     yield added

