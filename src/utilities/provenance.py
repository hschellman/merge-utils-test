''' provenance - use metacat to find provenance of a file'''

import os
import sys
import json
import argparse
from metacat.webapi import MetaCatClient
mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])
DEBUG = False
def prepend(item,alist):
    #print (item,alist)
    newlist =  [item] + alist
    #print (len(newlist),len(alist))
    return newlist

def makestep(filemeta):
    step = {}
    step["did"] = filemeta["namespace"] + ":" + filemeta["name"]
    step["fid"] = filemeta["fid"]
    step["namespace"] = filemeta["namespace"]
    step["name"] = filemeta["name"]
    step["metadata"] = filemeta["metadata"]
    metadata = filemeta["metadata"]
    step["data_tier"] = metadata["core.data_tier"]
    step["appversion"] = metadata["core.application.version"]
    step["appname"] = metadata["core.application.name"]
    #step["config_file"] = metadata["dune.config_file"]
    if "dune.config_file" in metadata:
        step["config_file"] = metadata["dune.config_file"]
    else:
        print ("WARNING: no dune.config_file in metadata for this step",step["appname"],step["appversion"])
        step["config_file"] = "unknown"
    if "dune_mc.generators" in metadata:
        step["generators"] = metadata["dune_mc.generators"]
    if "dune_mc.gen_fcl_filename" in metadata:  
        step["gen_fcl_filename"] = metadata["dune_mc.gen_fcl_filename"]
    if "dune_mc.geometry_version" in metadata:
        step["geometry_version"] = metadata["dune_mc.geometry_version"]
    step["appfamily"] = metadata["core.application.family"]
    if "parents" in filemeta:
        step["parents"] = filemeta["parents"]

    return step

def makestep_from_merge(filemeta):
    step = {}
    step["did"] = filemeta["namespace"] + ":" + filemeta["name"]
    step["fid"] = filemeta["fid"]
    step["namespace"] = filemeta["namespace"]
    step["name"] = filemeta["name"]
    step["metadata"] = filemeta["metadata"]
    metadata = filemeta["metadata"]
    step["data_tier"] = metadata["core.data_tier"]
    step["appversion"] = metadata["core.application.version"]
    step["appname"] = metadata["core.application.name"]
    step["config_file"] = metadata["merge.config_file"]
    step["appfamily"] = metadata["merge-utils"]
    if "parents" in filemeta:
        step["parents"] = filemeta["parents"]

    return step

def makestep_from_origins(thefilemeta, steps):
    ''' if no parents, look for origins in the metadata and make steps from those'''

    metadata = thefilemeta["metadata"]
    origins = metadata["origin.applications.config_files"]
    version = metadata["core.application.version"]
    for origin in origins:
        step = {}
        step["name"] = thefilemeta["name"]
        step["namespace"] = thefilemeta["namespace"]
        step["fid"] = thefilemeta["fid"]

        step["appname"] = origin
        step["appversion"] = version
        if "dune.config_file" in metadata:
            step["config_file"] = metadata["dune.config_file"]
        else:
            print ("WARNING: no dune.config_file in metadata for this step",step["appname"],step["appversion"])
            sys.exit(0)
        if "dune_mc.generators" in metadata:
            step["generators"] = metadata["dune_mc.generators"]
        if "dune_mc.gen_fcl_filename" in metadata:
            step["gen_fcl_filename"] = metadata["dune_mc.gen_fcl_filename"]
        if "dune_mc.geometry_version" in metadata:
            step["geometry_version"] = metadata["dune_mc.geometry_version"]
        step["name"]="null"
        steps = prepend(step,steps)

    return steps

def get_provenance(did =None,fid=None,steps=None):

    if did is not None and not ":" in did:
        print ("You must specify a did in the form of scope:name")
    
    try:
        if did is not None:
            thefilemeta = mc_client.get_file(did=did,with_metadata=True,with_provenance=True)
        elif fid is not None:
            thefilemeta = mc_client.get_file(fid=fid,with_metadata=True,with_provenance=True)
        else:
            print ("You must specify either a did or a fid")
            return None
    except Exception as e:
        print ("metacat query failed",e,did,fid)
        return None
    
    if DEBUG:
        print (json.dumps(thefilemeta,indent=4))

    # special case for merge steps, which have the config file in a different metadata field and don't have parents, so we want to make the step before looking for parents
    if "merge.config_file" in thefilemeta["metadata"]:
        try:
            step = makestep_from_merge(thefilemeta)
        except Exception as e:
            print ("Error occurred while making step from merge", e,fid,did)
            return steps
        steps = prepend(step, steps)
    else:
        try:
            if DEBUG: print ("making step from file",did,fid)
            step = makestep(thefilemeta)
        except Exception as e:
            print ("error:",e)
            print ("Error occurred while making step from file", e,fid,did)
            return steps
            print (json.dumps(step,indent=4))
        #steps[step["config_file"]] = step
        steps = prepend(step, steps)
    #print ("steps in get_provenance",len(steps))
    if "parents" in step and len(step["parents"]) > 0:
        if DEBUG: print ("has parents")
        steps = get_provenance(fid=step["parents"][0]["fid"],steps=steps)
    else:
        if DEBUG: print (" look for origins if no parents")
        if "origin.applications.config_files" in thefilemeta["metadata"]:
            steps = makestep_from_origins(thefilemeta = thefilemeta,steps=steps)
    return steps
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find the provenance of a file using metacat")
    parser.add_argument("--did", help="The did of the file to find the provenance of")
    parser.add_argument("--fid", help="The fid of the file to find the provenance of")
    parser.add_argument("--dataset", help="The dataset to find the provenance of (first file)")
    parser.add_argument("--query", help="Find provenance of the first file from this query")
    parser.add_argument("--test", help="Run a test of the provenance code", action="store_true")
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    if args.dataset:
        query = f"files from {args.dataset} limit 1"  
        files, = mc_client.query(query=query) 
        args.did = files["namespace"] + ":" + files["name"]
    if args.query:
        query = args.query + " limit 1" 
        files, = mc_client.query(query=query) 
        args.did = files["namespace"] + ":" + files["name"]
    if args.test:
        args.did ="fardet-hd:prodbackground_radiological_decay0_dune10kt_1x2x6_centralAPA_20250802T061831Z_gen_001175_supernova_g4_detsim_reco.root"
    print ("Looking at provenance for did:",args.did)
    allsteps=[]
    allsteps =get_provenance(did=args.did,fid=args.fid,steps=allsteps)
    #print ("provenance steps:",len(allsteps))
    gen = ""
    geo = ""
    for astep in allsteps:
        #print (json.dumps(astep,indent=4))
        if "gen_fcl_filename" in astep:
            gen = astep["gen_fcl_filename"]
        if "geometry_version" in astep:
            geo = astep["geometry_version"]
        if "generators" in astep:
            generator = astep["generators"]
        print (f"{astep['appname']:<10} {astep['data_tier']:<20} {astep['appversion']:<10}\t {astep['config_file']}" ) 
    if gen != "":
        print (f"gen_fcl_filename: {gen}")
    if geo != "":
        print (f"geometry_version: {geo}")
    if generator != "":
        print (f"generators:       {generator}")