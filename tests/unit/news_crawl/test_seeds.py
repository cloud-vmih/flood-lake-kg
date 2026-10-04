"""Test seeds: chuan hoa URL va dedup. Khong ra mang."""
from __future__ import annotations

from flashflood_data.news import seeds


def test_canonicalize_tracking_params_case_and_slash():
    assert (seeds.canonicalize_url(
        "https://WWW.VTV.VN/bai-viet.htm?utm_source=z&id=1")
        == "https://www.vtv.vn/bai-viet.htm?id=1")
    assert (seeds.canonicalize_url("https://vnexpress.net/tin-lu/")
            == "https://vnexpress.net/tin-lu")


def test_dedupe_keeps_first_and_order():
    rows = [{"url": "https://a.vn/x?utm_source=y", "n": "1"},
            {"url": "https://a.vn/x", "n": "2"},
            {"url": "https://b.vn/y", "n": "3"}]
    unique = seeds.dedupe_urls(rows)
    assert [r["n"] for r in unique] == ["1", "3"]


def test_load_seeds_skips_non_http(tmp_path):
    path = tmp_path / "seeds.csv"
    path.write_text("url,ten_nguon\nhttps://a.vn/x,Bao A\nnot-a-url,Bao B\n",
                    encoding="utf-8-sig")
    rows = seeds.dedupe_urls(seeds.load_seeds(str(path)))
    assert len(rows) == 1
    assert rows[0]["canonical_url"] == "https://a.vn/x"
