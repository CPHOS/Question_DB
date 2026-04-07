from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass
from pathlib import Path

from .config import REAL_TEST2_ZIP_PATH, REAL_TEST_ZIP_PATH
from .specs import PAPER_APPENDIX_SPECS, QUESTION_SPECS

PROBLEM_TITLE_RE = re.compile(
    r"\\begin\{problem\}(?:\[[^\]]*\])?\{(?P<title>[^{}]*)\}"
)


@dataclass
class RealQuestionFixture:
    slug: str
    upload_path: Path
    create_description: str
    create_difficulty: dict
    patch: dict
    asset_count: int
    title_hint: str
    source_dir_name: str


def build_sample_question_zips(samples_dir: Path) -> list[Path]:
    zip_paths: list[Path] = []
    for spec in QUESTION_SPECS:
        zip_path = samples_dir / spec["zip_name"]
        with zipfile.ZipFile(zip_path, "w") as archive:
            archive.writestr(spec["tex_name"], spec["tex_body"])
            archive.writestr("README.txt", "ignored root helper file\n")
            archive.writestr("assets/", b"")
            for asset_path, content in spec["assets"].items():
                archive.writestr(asset_path, content)
        zip_paths.append(zip_path)
    return zip_paths


def build_sample_paper_appendix_zips(
    samples_dir: Path,
    invalid_upload_path: Path,
) -> dict[str, Path]:
    zip_paths: dict[str, Path] = {}
    for spec in PAPER_APPENDIX_SPECS:
        zip_path = samples_dir / spec["zip_name"]
        with zipfile.ZipFile(zip_path, "w") as archive:
            for entry_path, content in spec["appendix_entries"].items():
                archive.writestr(entry_path, content)
        zip_paths[spec["slug"]] = zip_path
    invalid_upload_path.write_text("not a zip archive", encoding="utf-8")
    return zip_paths


def build_real_theory_question_zips(
    tmp_dir: Path, samples_dir: Path,
) -> list[RealQuestionFixture]:
    return _build_real_question_zips(
        tmp_dir=tmp_dir,
        samples_dir=samples_dir,
        zip_path=REAL_TEST_ZIP_PATH,
        archive_root_name="CPHOS2",
        extracted_root_name="real_theory_source",
        upload_prefix="real_theory",
        slug_prefix="real-theory",
        description_prefix="真实理论样题",
        title_fallback_prefix="theory",
        category="T",
        tag_prefixes=["theory", "real-batch"],
        create_notes_prefix="imported from test.zip folder",
        patch_notes_prefix="real theory fixture",
        expected_count=6,
    )


def build_real_experiment_question_zips(
    tmp_dir: Path, samples_dir: Path,
) -> list[RealQuestionFixture]:
    return _build_real_question_zips(
        tmp_dir=tmp_dir,
        samples_dir=samples_dir,
        zip_path=REAL_TEST2_ZIP_PATH,
        archive_root_name="CPHOS4-E",
        extracted_root_name="real_experiment_source",
        upload_prefix="real_experiment",
        slug_prefix="real-experiment",
        description_prefix="真实实验样题",
        title_fallback_prefix="experiment",
        category="E",
        tag_prefixes=["experiment", "real-exp-batch"],
        create_notes_prefix="imported from test2.zip folder",
        patch_notes_prefix="real experiment fixture",
        expected_count=4,
    )


def _build_real_question_zips(
    *,
    tmp_dir: Path,
    samples_dir: Path,
    zip_path: Path,
    archive_root_name: str,
    extracted_root_name: str,
    upload_prefix: str,
    slug_prefix: str,
    description_prefix: str,
    title_fallback_prefix: str,
    category: str,
    tag_prefixes: list[str],
    create_notes_prefix: str,
    patch_notes_prefix: str,
    expected_count: int,
) -> list[RealQuestionFixture]:
    assert zip_path.exists(), f"missing test fixture zip: {zip_path}"

    extracted_root = tmp_dir / extracted_root_name
    with zipfile.ZipFile(zip_path, "r") as archive:
        archive.extractall(extracted_root)

    base_dir = extracted_root / archive_root_name
    assert base_dir.exists(), f"expected directory missing: {base_dir}"

    upload_dir = samples_dir / "real_questions"
    upload_dir.mkdir(parents=True, exist_ok=True)

    fixtures: list[RealQuestionFixture] = []
    source_dirs = sorted(
        (p for p in base_dir.iterdir() if p.is_dir()),
        key=lambda p: int(p.name),
    )
    for index, source_dir in enumerate(source_dirs, start=1):
        tex_path = source_dir / "main.tex"
        assert tex_path.exists(), f"missing main.tex: {source_dir}"
        tex_body = tex_path.read_text(encoding="utf-8", errors="replace")
        title_hint = (
            _extract_problem_title(tex_body)
            or f"{title_fallback_prefix}-{source_dir.name}"
        )
        upload_path = upload_dir / f"{upload_prefix}_{source_dir.name}.zip"
        assets_dir = source_dir / "assets"
        asset_paths = (
            sorted(p for p in assets_dir.rglob("*") if p.is_file())
            if assets_dir.exists()
            else []
        )
        with zipfile.ZipFile(upload_path, "w") as archive:
            archive.writestr("main.tex", tex_body)
            archive.writestr("assets/", b"")
            for ap in asset_paths:
                archive.write(
                    ap, f"assets/{ap.relative_to(assets_dir).as_posix()}"
                )

        status = "reviewed" if index % 2 else "used"
        fixtures.append(
            RealQuestionFixture(
                slug=f"{slug_prefix}-{source_dir.name}",
                upload_path=upload_path,
                create_description=f"{description_prefix} {source_dir.name}",
                create_difficulty={
                    "human": {
                        "score": min(10, index + 3),
                        "notes": f"{create_notes_prefix} {source_dir.name}",
                    }
                },
                patch={
                    "category": category,
                    "description": f"{description_prefix} {source_dir.name}",
                    "tags": [*tag_prefixes, f"folder-{source_dir.name}"],
                    "status": status,
                    "author": f"命题人{index}",
                    "reviewers": [f"审题人{index}A", f"审题人{index}B"],
                    "difficulty": {
                        "human": {
                            "score": min(10, index + 4),
                            "notes": f"{patch_notes_prefix} {source_dir.name}",
                        },
                        "heuristic": {"score": min(10, index + 2)},
                    },
                },
                asset_count=len(asset_paths),
                title_hint=title_hint,
                source_dir_name=source_dir.name,
            )
        )

    assert len(fixtures) == expected_count, (
        f"expected {expected_count} from {zip_path.name}, got {len(fixtures)}"
    )
    return fixtures


def _extract_problem_title(tex_body: str) -> str | None:
    match = PROBLEM_TITLE_RE.search(tex_body)
    if not match:
        return None
    title = match.group("title").strip()
    return title or None
