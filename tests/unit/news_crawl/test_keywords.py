"""Test bo keyword: nap yaml, so khop, quyet dinh. Khong ra mang."""
from __future__ import annotations

import pytest

from flashflood_data.news import keywords

SAMPLE_YAML = """# comment
flood:
  - lũ quét
  - ngập
place_tinh:
  - sơn la
place_ban:
  - Co Mạ
exclude:
  - xả lũ
"""


@pytest.fixture(name="kw")
def kw_fixture(tmp_path):
    path = tmp_path / "keywords.yaml"
    path.write_text(SAMPLE_YAML, encoding="utf-8")
    return keywords.load_keywords(str(path))


def test_load_groups(kw):
    assert set(kw) == {"flood", "place_tinh", "place_ban", "exclude"}
    assert kw["flood"] == ["lũ quét", "ngập"]


def test_keep_flood_and_place(kw):
    hits = keywords.match("Lũ quét gây ngập tại Sơn La đêm qua.", kw)
    keep, reason = keywords.decide(hits)
    assert (keep, reason) == ("yes", "")
    assert "Sơn La" in keywords.place_hits(hits) or "sơn la" in keywords.place_hits(hits)


def test_exclude_wins_over_keep(kw):
    hits = keywords.match("Thủy điện xả lũ, nước ngập tại Sơn La.", kw)
    assert keywords.decide(hits) == ("no", "excluded")


def test_no_match_without_place(kw):
    assert keywords.decide(keywords.match("Mưa lũ gây ngập diện rộng.", kw)) == (
        "no", "no-match")


def test_commune_not_confused_with_common_words(kw):
    """'Co Mạ' (xa) khong duoc nham voi 'có mà' trong cau thuong."""
    hits = keywords.match("Tôi có mà đi từ sáng sớm.", kw)
    assert "Co Mạ" not in keywords.place_hits(hits)
    hits = keywords.match("Đoàn cứu hộ vào xã Co Mạ khắc phục sạt lở.", kw)
    assert "Co Mạ" in keywords.place_hits(hits)


def test_missing_exclude_group_behaves_as_off():
    kw = {"flood": ["lũ"], "place_tinh": ["sơn la"]}
    hits = keywords.match("Lũ về tại Sơn La.", kw)
    assert keywords.decide(hits) == ("yes", "")
