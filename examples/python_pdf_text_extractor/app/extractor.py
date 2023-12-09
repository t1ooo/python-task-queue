from datetime import timedelta
import enum
import uuid
import logging
import os
import tempfile
from typing import BinaryIO, Optional, Tuple
import magic
from python_task_queue.result import Ok
from python_task_queue.task import Options
from .tasks_backend import task_queue
from .tasks import extract_text_from_pdf


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PdfExtractorException(Exception):
    pass


def gen_uuid() -> str:
    return str(uuid.uuid4())


def is_pdf_file(f: BinaryIO) -> bool:
    return magic.from_descriptor(f.fileno()).startswith("PDF document")


_TEMPDIR = os.path.join(tempfile.gettempdir(), "python-pdf-text-extractor")


def upload_pdf(upload_filename: str, file: BinaryIO) -> str:
    if not is_pdf_file(file):
        raise PdfExtractorException("Not a pdf file.")

    id = gen_uuid()
    from_path = os.path.join(_TEMPDIR, f"{id}.pdf")
    to_path = os.path.join(_TEMPDIR, f"{id}.txt")

    os.makedirs(_TEMPDIR, exist_ok=True)
    with open(from_path, "wb") as f:
        f.write(file.read())

    task_queue.submit(
        extract_text_from_pdf,
        args=(upload_filename, from_path, to_path),
        opts=Options(id=id),
    )
    return id


class Status(str, enum.Enum):
    IN_PROGRESS = "IN_PROGRESS"
    FAILURE = "FAILURE"
    SUCCESS = "SUCCESS"


def task_status(task_id: str) -> Status | None:
    state = task_queue.state(task_id)
    if state is None:
        return None

    match state:
        case state.PENDING | state.RUNNING:
            return Status.IN_PROGRESS
        case state.SUCCESS:
            return Status.SUCCESS
        case state.FAILURE | state.CANCELLED | state.EXPIRED:
            return Status.FAILURE


def download(task_id: str, timeout: timedelta) -> Optional[Tuple[str, str]]:
    ar = task_queue.result(task_id, timeout)
    if type(ar) is Ok:
        upload_filename, to_path = ar.value
        return upload_filename + ".txt", to_path

    logger.error(ar)
    return None
