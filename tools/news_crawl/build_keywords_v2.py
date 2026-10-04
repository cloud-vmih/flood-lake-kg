#!/usr/bin/env python3
"""Sinh config/news_crawl/keywords_v2.yaml tu ranh xa 2025 + danh sach cung.

Chay: .venv/bin/python tools/news_crawl/build_keywords_v2.py
Bat buoc chay lai moi khi ranh hanh chinh thay doi; khong sua yaml bang tay.
"""
from __future__ import annotations

import os

import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
ADMIN_PARQUET = os.path.join(
    REPO_ROOT, "dataset", "harmonized", "admin",
    "admin_commune_2025.geoparquet")
OUT = os.path.join(REPO_ROOT, "config", "news_crawl", "keywords_v2.yaml")

FLOOD = ["lũ quét", "lũ ống", "ngập úng", "ngập", "sạt lở",
         "lũ lụt", "mưa lũ", "lũ", "vượt báo động", "báo động"]
EXCLUDE = ["xả lũ", "dự báo thời tiết", "xổ số", "giá vàng"]
RIVERS = ["sông mã", "nậm păm", "nậm pàn", "suối ban", "suối thì", "suối mong"]
VILLAGES = ["pá kìm", "bản bon", "bản phặng", "bản lẩy", "hua pư", "pá hốc",
            "pa kha", "ít ong", "hua trai", "mường trai", "mé bon",
            "bản kẻ", "bản công"]


def block(key: str, items: list[str], comment: str = "") -> str:
    lines = [f"# {comment}"] if comment else []
    lines.append(f"{key}:")
    lines += [f"  - {word}" for word in items]
    return "\n".join(lines)


def main() -> None:
    admin = pd.read_parquet(ADMIN_PARQUET)
    names = sorted({str(name).replace("Xã ", "").replace("Phường ", "").strip()
                    for name in admin["current_commune_name"]}, key=str.lower)
    assert len(names) == 75, f"so xa doi: {len(names)} (mong doi 75)"
    doc = ("# Keyword v2: sinh tu dong, khong sua tay.\n"
           "# Nguon dia danh: dataset/harmonized/admin/admin_commune_2025.geoparquet\n"
           "# Quy tac: flood AND place AND NOT exclude "
           "(exclude khuyen nghi chi lam co review: --no-exclude)\n")
    parts = [doc,
             block("flood", FLOOD),
             block("place_tinh", ["sơn la"]),
             block("place_xa_phuong", names, "75 xa/phuong 2025"),
             block("place_song_suoi", RIVERS),
             block("place_ban", VILLAGES, "ten ban tu bai bao da xac minh"),
             block("exclude", EXCLUDE, "cum loai tru (khop cum, khong khop tu don)")]
    with open(OUT, "w", encoding="utf-8") as handle:
        handle.write("\n".join(parts) + "\n")
    total = len(FLOOD) + 1 + len(names) + len(RIVERS) + len(VILLAGES) + len(EXCLUDE)
    print(f"xa/phuong: {len(names)} | tong tu: {total} -> {OUT}")


if __name__ == "__main__":
    main()
