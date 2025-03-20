Retrieving data
-	How to tell if data files are staged to disk?
    -	I got some methods that work, but only when I’m logged in at FNAL and looking at data on FNAL_DCACHE
    -	Is there a general method or do I need a special function for each RSE?
-	When to get files from tape vs copying from remote locations?
    -	E.g. ProtoDUNE data on disk at CERN and backed up to tape at FNAL
-	Do we have official guidelines for being a “good citizen” on the computers?
    -	Running multiple network file transfers simultaneously?
    -	Running scripts in the background on the GPVMs?
    -	Staging large amounts of data to disk at once?
-	Any special considerations for merging local files after analysis workflow?
    -	E.g. metadata json files in a separate folder from the data files

Running at different sites
-	The old scripts I got from Heidi assume they’ll run at FNAL, but I’d like the final version to be more general-purpose
    -	How standardized are the different sites?
    -	Any simple things to keep in mind for getting the merging working cross-site?
-	Where do we want the merging to be able to run?
    -	If it doesn’t have MetaCat or Rucio or access to the files then the merging will fail, but that’s not very user-friendly
    -	Probably we should have a whitelist of sites that are officially supported
-	If the data files are at a remote location it might be nice if we could submit a job to run at that site, and then just copy the final merged file back to the user’s site

Reusing existing frameworks
-	I’ve been playing with making my own system that finds files with MetaCat and Rucio and then copies them around with XRootD if necessary
    -	Am I reinventing the wheel?  Can I just use an existing system (justIN?) to get my input files together instead?
    -	Merging needs to map one job to many files instead of one to one, so it might be different from typical analysis jobs
-	Can we configure the merging to run automatically at the end of existing workflows?
-	None of the software workshops I have been to so far have said much about job submission.  I applied for the OSG school this summer, but are there any good resources I should look at in the meantime?
![image](https://github.com/user-attachments/assets/96f6d4ed-7963-4e38-bc00-dcb99de30e59)
