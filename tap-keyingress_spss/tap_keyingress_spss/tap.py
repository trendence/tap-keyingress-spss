"""keyingress_spss tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helper

from tap_keyingress_spss.streams import (
    keyingress_spssStream,
    InterviewStream,
    InterviewAnswerStream
)

STREAM_TYPES = [
    InterviewStream,
    InterviewAnswerStream
]

class Tapkeyingress_spss(Tap):
    """keyingress_spss tap class."""
    name = "tap-keyingress_spss"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "location_type",
            th.StringType,
            required=True,
            description="the type of location, where are the files located (either 'LOCAL' or 'GCP')"
        ),
        th.Property(
            "file_location",
            th.StringType,
            required=True,
            description="the folder containing the keyingress spss files"
        ),
        th.Property(
            "gcp_project",
            th.StringType,
            description="the gcp project which contains the bucket with the data; relevant if location_type=gcp"
        ),
        th.Property(
            "gcp_bucket",
            th.StringType,
            description="the gcp bucket which contains the data; relevant if location_type=gcp"
        ),
        th.Property(
            "gcp_credentials",
            th.StringType,
            description="path to gcp service account credentials; not needed if env var is set"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
