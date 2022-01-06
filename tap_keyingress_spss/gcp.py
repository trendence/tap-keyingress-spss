import logging
import fnmatch
from typing import List
import os
from datetime import datetime, timezone

from google.cloud import storage
from google.oauth2 import service_account

class CloudStorageClient():

    """
    A client to download blobs (files) from a given bucket on GCP Cloud Storage.
    """

    def __init__(self, project, bucket, credentials=None, skipIfExists=True):
        """
        constructor of the class

        bucket: name of the Cloud Storage bucket
        
        """
        self.bucket = bucket

        oauth_creds = None
        if credentials is not None:
            oauth_creds = service_account.Credentials.from_service_account_file(credentials)

        #init client
        self.storage_client = storage.Client(project=project, credentials=oauth_creds)

        self.skipIfExists = skipIfExists

    def download(self, path, fileFilter=None, targetPath="/tmp/") -> List[dict]:
        """
        Function to trigger the download. List all blobs with the
        given specifications, iterates over the blob list and downloads
        each files in a single request.

        path: subfolder where the files are located
        fileFilter: Unix style pathname pattern to filter files (see fnmatch or glob for details)
        targetPath: folder where files should be downloaded to, if not specified the local tmp folder is used

        return: list of file names which have been downloaded 
        """

        # list blobs
        blobs = self.listBlobs(path=path,fileFilter=fileFilter)

        # if no blobs could be found => exit
        if blobs is None or len(blobs) == 0:
            logging.warning(f"no blobs found at {self.bucket}/{path}")
            return None

        #iterate over blob list and download them
        files = []
        for b in blobs:
            files.append(self.downloadBlob(b, targetPath=targetPath))

        return files

    def listBlobs(self, path, fileFilter=None):
        """
        Lists blobs in a given bucket location. Filters the blobs
        if requested.

        path: subfolder where the files are located
        fileFilter: Unix style pathname pattern to filter files (see fnmatch or glob for details)

        return: list of absolute path to blobs
        """

        filenames = []
        
        #call api function to list blobs and add them to list
        blobs = self.storage_client.list_blobs(self.bucket,prefix=path)
        for blob in blobs:
            filenames.append(blob.name)

        #if filter is set => filter files with unix based match pattern
        if fileFilter is not None:
            return fnmatch.filter(filenames, fileFilter)
        #if no filter is set
        else:
            #exclude folder itself (first entry is folder)
            return filenames[1:]

    def downloadBlob(self, path:str, targetPath:str="/tmp/") -> dict:
        """
        Downloads a single blob from GCP Cloud Storage to a specified folder.

        path: path to the blob

        return name of the downloaded file (full path) as string
        """
        logging.info(f"loading {path} from {self.bucket} to {targetPath}")

        #instantiate bucket and blob object
        gsbucket = self.storage_client.get_bucket(self.bucket)
        blob = gsbucket.blob(path)

        #reload to update date information
        blob.reload()

        #specify tmp filename and create path to it
        tmpfile = path.replace("/","")
        tmpfile = f"{targetPath}{tmpfile}"

        doDownload = True
        if self.skipIfExists == True:
            if os.path.exists(tmpfile) == True:
                doDownload = False

        if doDownload:
            #download the blob to the specified filename
            blob.download_to_filename(tmpfile)
            logging.info(f"LOADING DONE - {path} from {self.bucket}")
        else:
            logging.info(f"{path} got already loaded from {self.bucket} - SKIPPING")

        b = {}
        b["local_filename"] = tmpfile
        if blob.updated is not None:
            b["modified"] = blob.updated
        else:
            b["modified"] = blob.time_created
        b["name"] = f"gs://{self.bucket}/{blob.name}"

        return b