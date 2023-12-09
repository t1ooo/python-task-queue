from dataclasses import dataclass
from datetime import timedelta
from typing import TypeVar, Generic, Union
from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import app.extractor as extractor

app = FastAPI()


@dataclass
class ErrorResponse:
    error: str


T = TypeVar("T")


@dataclass
class OkResponse(Generic[T]):
    data: T


@app.exception_handler(RequestValidationError)
async def exception_handler(e: RequestValidationError):
    return ErrorResponse(str(e))


@app.post("/upload-pdf")
async def upload_pdf(file: UploadFile) -> Union[OkResponse[str], ErrorResponse]:
    try:
        upload_filename = file.filename or ""
        id = extractor.upload_pdf(upload_filename, file.file)
        return OkResponse[str](id)
    except extractor.PdfExtractorException as e:
        return ErrorResponse(str(e))


@dataclass
class Task:
    id: str


@app.post("/task-status")
async def task_status(task: Task) -> OkResponse[extractor.Status]:
    status = extractor.task_status(task.id)
    if status is None:
        raise HTTPException(status_code=404)
    return OkResponse[extractor.Status](status)


@app.get("/download/{id}")
async def download(id: str) -> FileResponse:
    result = extractor.download(id, timedelta(seconds=10))
    if result is None:
        raise HTTPException(status_code=404)
    download_filename, filepath = result
    return FileResponse(
        filepath, media_type="application/octet-stream", filename=download_filename
    )


app.mount("/", StaticFiles(directory="static", html=True), "static")
