#!/usr/bin/env python3
"""Check that relative links between markdown files in this repository resolve.

The website has its own link checker, but it only sees the pages it publishes: the docs under
docs/, after the sync rewrites them. Nothing checks the root README, CONTRIBUTING, or a link that
points at a source file rather than another doc, which is most of what the README does.

Both file targets and heading anchors are checked, because a link to a section that was later
renamed is the failure people actually hit.

Usage: hack/check-doc-links.py [--root DIR]
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

SKIP_DIRS = {".git", "node_modules", "dist", ".astro", "_substrate", "vendor"}

# Inline links, but not images, and not autolinks. Reference-style links are not used here.
LINK = re.compile(r"(?<!\!)\[(?P<text>[^\]]*)\]\((?P<target>[^)\s]+)(?:\s+\"[^\"]*\")?\)")
ATX_HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*$", re.MULTILINE)
HTML_ANCHOR = re.compile(r'<a\s+[^>]*name="([^"]+)"', re.IGNORECASE)
FENCE = re.compile(r"^```.*?^```", re.MULTILINE | re.DOTALL)


def slugify(heading: str) -> str:
    """Approximate GitHub's heading slugs: strip formatting, lowercase, hyphenate."""
    text = re.sub(r"`([^`]*)`", r"\1", heading)
    text = re.sub(r"\*\*?([^*]*)\*\*?", r"\1", text)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    return re.sub(r"\s+", "-", text)


def anchors_of(path: Path) -> set[str]:
    """Every anchor a link can target in a markdown file."""
    if not path.exists() or path.suffix != ".md":
        return set()
    body = FENCE.sub("", path.read_text(encoding="utf-8", errors="replace"))
    found = {slugify(h) for _, h in ATX_HEADING.findall(body)}
    # Explicit <a name="..."> anchors keep their case, unlike heading slugs, so index both forms
    # and compare case-insensitively rather than assuming one convention.
    found |= set(HTML_ANCHOR.findall(body))
    return {a.lower() for a in found}


def markdown_files(root: Path) -> list[Path]:
    return sorted(
        p
        for p in root.rglob("*.md")
        if not any(part in SKIP_DIRS for part in p.relative_to(root).parts)
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", default=".", help="repository root to scan")
    args = ap.parse_args()
    root = Path(args.root).resolve()

    files = markdown_files(root)
    anchor_cache: dict[Path, set[str]] = {}
    failures: list[str] = []
    checked = 0

    for path in files:
        body = FENCE.sub("", path.read_text(encoding="utf-8", errors="replace"))
        rel = path.relative_to(root)
        for match in LINK.finditer(body):
            target = match.group("target")
            if target.startswith(("http://", "https://", "mailto:", "tel:")):
                continue
            checked += 1
            file_part, _, anchor = target.partition("#")

            if not file_part:  # same-file anchor
                dest = path
            else:
                dest = (path.parent / file_part).resolve()
                if not dest.exists():
                    failures.append(f"{rel}: missing target {target}")
                    continue

            if anchor:
                if dest not in anchor_cache:
                    anchor_cache[dest] = anchors_of(dest)
                known = anchor_cache[dest]
                # Only markdown exposes headings; a link into source code cannot be checked.
                if dest.suffix == ".md" and anchor.lower() not in known:
                    failures.append(f"{rel}: no anchor #{anchor} in {file_part or rel.name}")

    if failures:
        print(f"broken links ({len(failures)}):", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        return 1

    print(f"checked {checked} relative links across {len(files)} markdown files — all resolve")
    return 0


if __name__ == "__main__":
    sys.exit(main())
