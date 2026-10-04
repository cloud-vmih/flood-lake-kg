"""Boc chu bai bao: uu tien <article>, bo furniture (sidebar, tin lien
quan, widget, comment, quang cao). Tra ve (title, text, scope)."""
from __future__ import annotations

import re
from html.parser import HTMLParser


class TextParser(HTMLParser):
    SKIP_TAGS = ("script", "style", "nav", "header", "footer", "aside")
    VOID_TAGS = ("br", "img", "meta", "link", "hr", "input", "source", "wbr")
    FURNITURE_RE = re.compile(
        r"sidebar|related|lien-quan|widget|comment|binh-luan|advert|quang-cao|"
        r"banner|popup|weather|thoi-tiet|tien-ich|tag-box|author-box|share",
        re.IGNORECASE)

    def __init__(self) -> None:
        super().__init__()
        self.title = ""
        self._in_title = False
        self._stack: list[bool] = []
        self._in_article = 0
        self._chunks: list[str] = []
        self._article_chunks: list[str] = []

    def _is_furniture(self, attrs: list[tuple[str, str | None]]) -> bool:
        for key, value in attrs:
            if key in ("class", "id") and self.FURNITURE_RE.search(value or ""):
                return True
        return False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "title":
            self._in_title = True
        if tag == "article":
            self._in_article += 1
        bad = tag in self.SKIP_TAGS or self._is_furniture(attrs)
        parent_bad = bool(self._stack and self._stack[-1])
        if tag not in self.VOID_TAGS:
            self._stack.append(bad or parent_bad)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        pass  # the <x/> tu dong: khong day stack

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False
        if tag == "article" and self._in_article:
            self._in_article -= 1
        if self._stack:
            self._stack.pop()

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title += data
        elif not (self._stack and self._stack[-1]):
            text = data.strip()
            if len(text) > 20:
                if self._in_article:
                    self._article_chunks.append(text)
                else:
                    self._chunks.append(text)

    def body(self) -> tuple[str, str]:
        """Tra ve (text, scope): 'article' neu co <article>, khong thi 'fallback'."""
        picks = self._article_chunks or self._chunks
        scope = "article" if self._article_chunks else "fallback"
        return (self.title + "\n" + "\n".join(picks)).strip(), scope


def extract(html: str) -> dict[str, object]:
    """Boc 1 trang HTML thanh {title, text, text_len, scope}."""
    parser = TextParser()
    parser.feed(html)
    text, scope = parser.body()
    return {"title": parser.title.strip()[:150], "text": text,
            "text_len": len(text), "scope": scope}
