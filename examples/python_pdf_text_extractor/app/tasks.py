from typing import Tuple
from pdfminer.high_level import extract_text_to_fp


def extract_text_from_pdf(
    upload_filename: str, from_path: str, to_path: str
) -> Tuple[str, str]:
    with open(from_path, "rb") as f, open(to_path, "wb") as t:
        extract_text_to_fp(f, t)
    return (upload_filename, to_path)
