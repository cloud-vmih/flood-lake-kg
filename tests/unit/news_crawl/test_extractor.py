"""Test boc chu: article-scope, bo furniture, fallback. Khong ra mang."""
from __future__ import annotations

import os

from flashflood_data.news import extractor

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "..", "fixtures", "news_crawl")


def read(name: str) -> str:
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as handle:
        return handle.read()


def test_article_scope_drops_sidebar_and_nav():
    parsed = extractor.extract(read("article_sample.html"))
    assert parsed["scope"] == "article"
    assert "Mường La" in parsed["text"]
    assert "Giá vàng" not in parsed["text"]
    assert "Dự báo thời tiết" not in parsed["text"]
    assert "xả lũ" not in parsed["text"]
    assert parsed["title"].startswith("Mưa lũ gây thiệt hại")


def test_fallback_without_article_tag():
    parsed = extractor.extract(read("fallback_sample.html"))
    assert parsed["scope"] == "fallback"
    assert "Pá Kìm" in parsed["text"]
    assert "Quảng cáo" not in parsed["text"]


def test_void_tags_do_not_break_stack():
    html = ("<html><head><title>T</title></head><body><div><br><img src='a'>"
            "<p>Bài viết dài về trận lũ quét đêm qua tại vùng cao.</p>"
            "</div></body></html>")
    parsed = extractor.extract(html)
    assert "lũ quét" in parsed["text"]
