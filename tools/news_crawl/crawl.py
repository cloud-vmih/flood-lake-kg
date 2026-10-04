#!/usr/bin/env python3
"""Chay tay triage tin lu: doc seeds -> tai -> boc -> soi keyword -> ghi CSV/JSON.

Khong can Docker. Vi du:
  .venv/bin/python tools/news_crawl/crawl.py --limit 5
  .venv/bin/python tools/news_crawl/crawl.py --all --no-exclude
Ket qua mac dinh vao dataset/news_crawl/outputs/ (nam trong dataset/
gitignore, may nao cung co, khong commit, ngoai state Docker).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import UTC, datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "src"))

from flashflood_data.news import extractor, fetcher, keywords, seeds

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))


def main() -> None:
    parser = argparse.ArgumentParser(description="Triage tin lu Son La (chay tay)")
    parser.add_argument("--seeds",
                        default=os.path.join(REPO_ROOT, "config", "news_crawl",
                                             "seeds.csv"))
    parser.add_argument("--keywords",
                        default=os.path.join(REPO_ROOT, "config", "news_crawl",
                                             "keywords_v2.yaml"))
    parser.add_argument("--limit", type=int, default=5,
                        help="so URL dau tien de thu (bo qua de chay het)")
    parser.add_argument("--all", action="store_true", help="chay het seeds")
    parser.add_argument("--no-exclude", action="store_true",
                        help="exclude chi lam co review, khong auto-loai")
    parser.add_argument("--out-dir",
                        default=os.path.join(REPO_ROOT, "dataset", "news_crawl",
                                             "outputs"))
    args = parser.parse_args()

    rows = seeds.dedupe_urls(seeds.load_seeds(args.seeds))
    if not args.all:
        rows = rows[:args.limit]
    print(f"targets: {len(rows)} URL")

    kw = keywords.load_keywords(args.keywords)
    if args.no_exclude:
        kw.pop("exclude", None)
    print(f"keywords: {sum(len(v) for v in kw.values())} tu")

    polite = fetcher.PoliteFetcher()
    out = []
    for row in rows:
        url = row["url"]
        fetched = polite.get(url)
        rec = {"label": row.get("nhan_su_kien", "")[:60],
               "outlet": row.get("ten_nguon", ""), "url": url,
               "canonical_url": row.get("canonical_url", ""),
               "status": "ok" if fetched.ok else "fail",
               "http": fetched.http, "elapsed_s": fetched.elapsed_s,
               "title": "", "text_len": 0, "scope": "",
               "flood_hits": "", "place_hits": "", "exclude_hits": "",
               "keep": "", "keep_reason": "", "error": fetched.error}
        if fetched.ok:
            parsed = extractor.extract(fetched.html)
            hits = keywords.match(parsed["text"], kw)
            keep, reason = keywords.decide(hits)
            rec.update(title=parsed["title"], text_len=parsed["text_len"],
                       scope=parsed["scope"],
                       flood_hits="|".join(hits.get("flood", [])),
                       place_hits="|".join(keywords.place_hits(hits)),
                       exclude_hits="|".join(hits.get("exclude", [])),
                       keep=keep, keep_reason=reason)
        out.append(rec)
        print(f"[{rec['status']}] {rec['label'][:40]} | len={rec['text_len']} "
              f"keep={rec['keep']}({rec['keep_reason']}) scope={rec['scope']}")
    os.makedirs(args.out_dir, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    json_path = os.path.join(args.out_dir, f"crawl_result_{stamp}.json")
    csv_path = os.path.join(args.out_dir, f"crawl_result_{stamp}.csv")
    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(out, handle, ensure_ascii=False, indent=2)
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(out[0].keys()))
        writer.writeheader()
        writer.writerows(out)
    ok = sum(1 for r in out if r["status"] == "ok")
    kept = sum(1 for r in out if r["keep"] == "yes")
    print(f"[DONE] fetch {ok}/{len(out)}, keep {kept}/{len(out)}")
    print(f"  JSON: {json_path}\n  CSV : {csv_path}")


if __name__ == "__main__":
    main()
