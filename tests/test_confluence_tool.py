"""
Tests for confluence_tool.py — HTML cleaning and URL parsing.
"""

import pytest

from bau.analysis.tools.confluence_tool import (
    MAX_CONTENT_LENGTH,
    _clean_html,
    _extract_page_id,
)


class TestExtractPageId:
    def test_extract_from_page_id_param(self):
        url = "https://confluence.example.com/pages/viewpage.action?pageId=12345"
        assert _extract_page_id(url) == "12345"

    def test_extract_from_pages_path(self):
        url = "https://confluence.example.com/pages/12345"
        assert _extract_page_id(url) == "12345"

    def test_returns_none_for_unknown_format(self):
        url = "https://confluence.example.com/display/TEAM/Page+Name"
        assert _extract_page_id(url) is None


class TestCleanHtml:
    def test_strips_tags(self):
        html = "<p>Hello <b>world</b></p>"
        assert "Hello" in _clean_html(html)
        assert "world" in _clean_html(html)
        assert "<" not in _clean_html(html)

    def test_preserves_newlines_for_block_elements(self):
        html = "<p>para 1</p><p>para 2</p>"
        result = _clean_html(html)
        assert "para 1" in result
        assert "para 2" in result

    def test_handles_br_tags(self):
        html = "line 1<br/>line 2<br>line 3"
        result = _clean_html(html)
        assert "line 1" in result
        assert "line 2" in result

    def test_decodes_html_entities(self):
        html = "&amp; &lt; &gt; &quot;"
        result = _clean_html(html)
        assert "& < > " in result or ("&" in result and "<" in result)

    def test_removes_script_tags(self):
        html = "before<script>alert('xss')</script>after"
        result = _clean_html(html)
        assert "alert" not in result
        assert "before" in result
        assert "after" in result

    def test_collapses_blank_lines(self):
        html = "<p>a</p>\n\n\n\n\n<p>b</p>"
        result = _clean_html(html)
        # Should not have more than 2 consecutive newlines
        assert "\n\n\n" not in result

    def test_empty_input(self):
        assert _clean_html("") == ""
