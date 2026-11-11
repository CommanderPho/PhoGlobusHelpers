"""Unit tests for standalone session log browser HTML wrap / save / open."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import webbrowser

from phoglobushelpers.gen_scripts_log_status import (
    _SESSION_LOG_BROWSER_CSS,
    _open_html_in_default_browser,
    _save_session_log_browser_html,
    _wrap_session_log_browser_document,
)


def test_wrap_session_log_browser_document_embeds_fragment():
    fragment = '<div class="gen-scripts-log-browser">hello</div>'
    doc = _wrap_session_log_browser_document(fragment, title="My Logs <x>")
    assert doc.startswith("<!DOCTYPE html>")
    assert "<meta charset=\"utf-8\"/>" in doc
    assert "<title>My Logs &lt;x&gt;</title>" in doc
    assert "background: #1e1e1e" in doc
    assert "color: #cccccc" in doc
    assert "body { padding: 8px; box-sizing: border-box; }" in doc
    assert fragment in doc


def test_session_log_browser_css_dark_fallbacks():
    css = _SESSION_LOG_BROWSER_CSS
    assert "var(--jp-layout-color0, #1e1e1e)" in css
    assert "var(--jp-ui-font-color1, #cccccc)" in css
    assert "var(--jp-border-color1, #3c3c3c)" in css
    assert "var(--jp-layout-color2, #094771)" in css
    assert "var(--jp-layout-color1, #252526)" in css
    assert "var(--jp-layout-color2, #3a3d41)" in css
    assert "var(--jp-layout-color3, #45494e)" in css
    assert "button:hover:not(.is-active)" in css
    assert "background: #2a2d2e;" in css
    # Main shell must not fall back to transparent/inherit outside the host.
    assert "var(--jp-layout-color0, transparent)" not in css
    assert "var(--jp-ui-font-color1, inherit)" not in css


def test_save_session_log_browser_html_writes_utf8(tmp_path: Path):
    fragment = "<div>café · logs</div>"
    out = tmp_path / "nested" / "browser.html"
    written = _save_session_log_browser_html(fragment, path=out, title="T")
    assert written == out
    text = out.read_text(encoding="utf-8")
    assert "<!DOCTYPE html>" in text
    assert fragment in text
    assert "<title>T</title>" in text


def test_save_session_log_browser_html_default_temp_path():
    fragment = "<div>x</div>"
    written = _save_session_log_browser_html(fragment)
    try:
        assert written.name.startswith("gen_scripts_log_browser_")
        assert written.suffix == ".html"
        assert written.is_file()
        assert "<div>x</div>" in written.read_text(encoding="utf-8")
    finally:
        written.unlink(missing_ok=True)


def test_open_html_in_default_browser_calls_webbrowser(tmp_path: Path):
    path = tmp_path / "page.html"
    path.write_text("<html></html>", encoding="utf-8")
    with patch.object(webbrowser, "open_new_tab") as mock_open:
        _open_html_in_default_browser(path)
        mock_open.assert_called_once_with(path.resolve().as_uri())


def test_open_html_in_default_browser_handles_error(tmp_path: Path, capsys):
    path = tmp_path / "page.html"
    path.write_text("<html></html>", encoding="utf-8")
    with patch.object(webbrowser, "open_new_tab", side_effect=webbrowser.Error("none")):
        _open_html_in_default_browser(path)
    captured = capsys.readouterr()
    assert "No browser available" in captured.out
