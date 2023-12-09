import time
import requests

from app.extractor import Status

BASE_URL = "http://127.0.0.1:8000"


def test_api():
    # upload
    filename = "tests/test_data/test.pdf"
    with open(filename, "rb") as file:
        response = requests.post(BASE_URL + "/upload-pdf", files={"file": file})
        id = response.json().get("data")
        assert response.status_code == 200
        assert isinstance(id, str)
        assert len(id) > 0

    # read status
    data = ""
    for _ in range(10):
        response = requests.post(BASE_URL + "/task-status", json={"id": id})
        data = response.json().get("data", None)
        assert response.status_code == 200
        assert data in [v.value for v in Status]
        if data == Status.SUCCESS:
            break
        time.sleep(1)
    assert data == Status.SUCCESS

    # download file
    response = requests.get(BASE_URL + "/download/" + id)
    assert response.status_code == 200
    assert response.content.decode().strip() == "some data"


def test_upload_non_pdf_file():
    filename = "tests/test_data/test.txt"
    url = BASE_URL + "/upload-pdf"
    with open(filename, "rb") as file:
        response = requests.post(url, files={"file": file})
        data = response.json().get("error", None)
        assert response.status_code == 200
        assert isinstance(data, str)
        assert data == "Not a pdf file."


def test_download_non_existent_id():
    id = "12345"
    response = requests.get(BASE_URL + "/download/" + id)
    assert response.status_code == 404
    assert response.json() == {"detail": "Not Found"}
