"""Bo keyword: nap yaml, so khop, quyet dinh giu/loai.

Quy tac: flood AND place AND NOT exclude. Nhom place_* so tren chu
CO DAU (casefold) de 'Co Mạ' khong nham voi 'có mà'; flood/exclude so
tren chu khong dau. Nhom exclude vang mat thi coi nhu tat.
"""
from __future__ import annotations

import re
import unicodedata


def load_keywords(path: str) -> dict[str, list[str]]:
    """Doc yaml con (group: + danh sach '- ...'), khong can pyyaml."""
    groups: dict[str, list[str]] = {}
    current: str | None = None
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped.endswith(":") and not stripped.startswith("-"):
                current = stripped[:-1]
                groups[current] = []
            elif stripped.startswith("- ") and current:
                groups[current].append(stripped[2:].strip())
    return groups


def nodash(text: str) -> str:
    """Ha chu co dau thanh khong dau de so tu flood."""
    decomposed = unicodedata.normalize("NFD", text.lower())
    return "".join(c for c in decomposed if unicodedata.category(c) != "Mn")


def match(raw_text: str, keywords: dict[str, list[str]]) -> dict[str, list[str]]:
    """Tra ve hit theo tung nhom."""
    norm = nodash(raw_text)
    low = raw_text.casefold()
    hits: dict[str, list[str]] = {}
    for group, words in keywords.items():
        if group.startswith("place") or group == "place":
            hits[group] = [w for w in words
                           if re.search(r"(?<!\w)" + re.escape(w.casefold())
                                        + r"(?!\w)", low)]
        else:
            hits[group] = [w for w in words
                           if re.search(r"(?<!\w)" + re.escape(nodash(w))
                                        + r"(?!\w)", norm)]
    return hits


def place_hits(hits: dict[str, list[str]]) -> list[str]:
    return [w for group, words in hits.items()
            if group.startswith("place") or group == "place" for w in words]


def decide(hits: dict[str, list[str]]) -> tuple[str, str]:
    """Tra ve (keep, reason): 'yes'/'no', ly do 'excluded'/'no-match'."""
    flood = hits.get("flood", [])
    place = place_hits(hits)
    excluded = hits.get("exclude", [])
    if flood and place and not excluded:
        return "yes", ""
    if excluded:
        return "no", "excluded"
    return "no", "no-match"
