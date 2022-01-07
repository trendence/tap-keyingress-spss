"""Custom client handling, including keyingress_spssStream base class."""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk.streams import Stream

import pyreadstat
import logging

from tap_keyingress_spss.gcp import CloudStorageClient
from tap_keyingress_spss.spss import InterviewCollector, QuestionCollector


class keyingress_spssStream(Stream):
    """Stream class for keyingress_spss streams."""

    LOCATION_GCP = "GCP"
    LOCATION_LOCAL = "LOCAL"

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        logging.info(f"THIS IS THE CONFIG: {self._config}")
        logging.info(f"AND THE NAME {self.name}")

        for sf in self.get_files():
            logging.info(f"FILE: {sf}")
            records = self.read_spss_file_records(sf)
            for r in records:
                yield r

    def get_files(self) -> List[str]:
        location_type = self._config.get('location_type')
        file_location = self.config.get('file_location')
        if location_type == self.LOCATION_GCP:
            gcpCsClient = CloudStorageClient(project=self._config.get('gcp_project'),
                                             bucket=self._config.get('gcp_bucket'),
                                             credentials=self._config.get('gcp_credentials'))
            return gcpCsClient.download(path=file_location, fileFilter="*.sav")
        elif location_type == self.LOCATION_LOCAL:
            #read local folder
            return self.get_local_files(file_location)
        else:
            raise NotImplementedError(f"location type {location_type} not implemented or unknown")

    def get_local_files(self, file_location):
        #FIXME: recursive
        #FIXME: filter on 'sav' files
        files = []
        for f in os.listdir(file_location):
            filename = os.path.join(file_location, f)
            modified = datetime.fromtimestamp(os.path.getmtime(filename), tz=timezone.utc)
            files.append({'local_filename': filename,
                     'name': filename,
                     'modified': modified})
        return files

    def read_spss_file_records(self, file:dict):
        try:
            df, meta = pyreadstat.read_sav(file.get('local_filename'))
            ic = InterviewCollector(
                modified=file.get('modified'),
                source_file=file.get('name'))
            qc = QuestionCollector(
                df=df, 
                meta=meta, 
                modified=file.get('modified'),
                source_file=file.get('name'))
        except pyreadstat._readstat_parser.ReadstatError:
            logging.error(f"pyreadstat could not read {file}")
            raise ValueError(f"pyreadstat could not read {file}")

        logging.info(f"NO OF RECORDS TO BE PROCESSED: {df.shape[0]}")

        if self.name == 'interview':
            return ic.collectInterviews(df=df)
        elif self.name == 'interview_answer':
            return ic.collectInterviewAnswers(df=df, meta=meta)
        elif self.name == 'question':
            return qc.getQuestions()
        elif self.name == 'question_option':
            return qc.getOptions()
        else:
            not NotImplementedError(f"stream {self.name} unknown")
