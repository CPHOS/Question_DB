#!/usr/bin/env python3
"""Assemble the combined API documentation from per-module API.md files.

Usage:
    python scripts/build_api_doc.py          # writes docs/API.md
    python scripts/build_api_doc.py --check  # exits non-zero if docs/API.md is stale
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HEADER = ROOT / "docs" / "API_HEADER.md"
OUTPUT = ROOT / "docs" / "API.md"

# Ordered list of (section_title, path_relative_to_ROOT).
# section_title is used as a ## heading in the assembled doc.
MODULES: list[tuple[str, str]] = [
    ("System — 系统", "src/api/system/API.md"),
    ("Auth — 认证", "src/api/auth/API.md"),
    ("Questions — 题目", "src/api/questions/API.md"),
    ("Papers — 试卷", "src/api/papers/API.md"),
    ("Ops — 运维操作", "src/api/ops/API.md"),
    ("Admin — 管理员", "src/api/admin/API.md"),
]

_HEADING_RE = re.compile(r"^(#{1,5}) ", re.MULTILINE)


def _downgrade_headings(text: str) -> str:
    """Add one '#' to every Markdown heading (# → ##, ## → ###, …).

    Only touches lines that start with 1-5 '#' characters so we never
    exceed the 6-level Markdown limit.
    """
    return _HEADING_RE.sub(lambda m: m.group(1) + "# ", text)


def _rewrite_relative_links(text: str, module_dir: str) -> str:
    """Rewrite relative Markdown links so they resolve from docs/.

    For example a link in src/api/ops/API.md pointing to
    ``../questions/API.md`` becomes ``../src/api/questions/API.md``.
    """
    def _replace(m: re.Match) -> str:
        label = m.group(1)
        target = m.group(2)
        # Skip absolute URLs and anchors.
        if target.startswith(("http://", "https://", "#")):
            return m.group(0)
        # Resolve relative to source dir, then make relative to docs/.
        resolved = (Path(module_dir) / target).resolve()
        try:
            from_docs = resolved.relative_to(ROOT)
        except ValueError:
            return m.group(0)
        return f"[{label}](../{from_docs.as_posix()})"

    return re.sub(r"\[([^\]]*)\]\(([^)]+)\)", _replace, text)


def build() -> str:
    parts: list[str] = []

    # --- header ---
    header_text = HEADER.read_text(encoding="utf-8").rstrip("\n")
    parts.append(header_text)

    # --- table of contents ---
    toc_lines = ["\n\n## 目录\n"]
    for title, _ in MODULES:
        anchor = title.lower().replace(" ", "-").replace("—", "").replace("--", "-").strip("-")
        toc_lines.append(f"- [{title}](#{anchor})")
    parts.append("\n".join(toc_lines))

    # --- modules ---
    for title, rel_path in MODULES:
        src = ROOT / rel_path
        if not src.exists():
            print(f"WARNING: {src} not found, skipping", file=sys.stderr)
            continue
        raw = src.read_text(encoding="utf-8").rstrip("\n")
        module_dir = str(Path(rel_path).parent)

        # Strip the leading `# Title` line — we provide our own ## heading.
        raw = re.sub(r"^# [^\n]+\n*", "", raw, count=1)

        body = _downgrade_headings(raw)
        body = _rewrite_relative_links(body, module_dir)
        parts.append(f"\n\n---\n\n## {title}\n\n{body}")

    return "\n".join(parts) + "\n"


def main() -> None:
    content = build()

    if "--check" in sys.argv:
        if not OUTPUT.exists():
            print(f"ERROR: {OUTPUT} does not exist. Run without --check to generate.", file=sys.stderr)
            sys.exit(1)
        existing = OUTPUT.read_text(encoding="utf-8")
        if existing != content:
            print(f"ERROR: {OUTPUT} is stale. Regenerate with: python scripts/build_api_doc.py", file=sys.stderr)
            sys.exit(1)
        print("docs/API.md is up to date.")
        return

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(content, encoding="utf-8")
    print(f"Generated {OUTPUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
