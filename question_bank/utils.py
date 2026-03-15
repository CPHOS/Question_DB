from __future__ import annotations

import hashlib
import json
import re
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

XLSX_MIME_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
TEXT_ENCODINGS = ("utf-8-sig", "utf-8", "gb18030")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def load_text(path: Path) -> str:
    for encoding in TEXT_ENCODINGS:
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    return path.read_text(encoding="utf-8", errors="replace")


def normalize_search_text(*parts: str | None, limit: int = 1000) -> str:
    text = " ".join(part for part in parts if part)
    text = re.sub(r"\\[A-Za-z@]+", " ", text)
    text = re.sub(r"[{}\[\]$^_&%#~]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


def dumps_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(content: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(content)
    return digest.hexdigest()


def xlsx_sheet_names(path: Path) -> list[str]:
    namespace = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(path) as archive:
        root = ET.fromstring(archive.read("xl/workbook.xml"))
        sheets = root.find("a:sheets", namespace)
        if sheets is None:
            return []
        return [sheet.attrib.get("name", "") for sheet in sheets]
