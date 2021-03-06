"""Stream type classes for tap-keyingress_spss."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_keyingress_spss.client import keyingress_spssStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class InterviewStream(keyingress_spssStream):
    """Define custom stream."""
    name = "interview"
    primary_keys = ["source_file","i_NUMBER"]
    replication_key = "file_modified"
    schema_filepath = SCHEMAS_DIR / "interview.json"

class InterviewAnswerStream(keyingress_spssStream):
    """Define custom stream."""
    name = "interview_answer"
    primary_keys = ["source_file","i_NUMBER","question_id"]
    replication_key = "file_modified"
    schema_filepath = SCHEMAS_DIR / "interview_answer.json"

class QuestionStream(keyingress_spssStream):
    """Define custom stream."""
    name = "question"
    primary_keys = ["source_file","question_id"]
    replication_key ="file_modified"
    schema_filepath = SCHEMAS_DIR / "question.json"

class QuestionOptionStream(keyingress_spssStream):
    """Define custom stream."""
    name = "question_option"
    primary_keys = ["source_file","question_id","option_id"]
    replication_key ="file_modified"
    schema_filepath = SCHEMAS_DIR / "question_option.json"