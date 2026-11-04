"""Unit tests for session log browser left-tab status / display-name labels."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from phoglobushelpers.gen_scripts_log_status import (
    _build_session_log_browser_html,
    _count_running_jobs_for_session_logs,
    _session_tab_display_names,
    _session_tab_status_token,
)


def _fresh_mtime(minutes_ago: float = 5.0) -> datetime:
    """Mtime within ``_RUNNING_AGE_HOURS`` of wall clock so Running classification sticks."""
    return datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)


_RUNNING_LOG = "[TIMING START] batch_load_session @ 2026-11-04T11:50:22\nworking...\n"
_COMPLETED_LOG = (
    "[TIMING START] batch_load_session @ 2026-11-04T10:00:00\n"
    "finished run: SessionBatchProgress.COMPLETED\n"
    "errors: None\n"
    "2026-11-04 10:30:00,000 logger]  [INFO ]\n"
    " ALL DONE!\n"
)


def test_session_tab_status_token_finished_with_running():
    assert _session_tab_status_token("finished", 2) == "+2"
    assert _session_tab_status_token("finished", 0) == "finished"
    assert _session_tab_status_token("running", 1) == "running"
    assert _session_tab_status_token("failed", 3) == "failed"


def test_session_tab_display_names_strips_when_unique():
    paths = [
        r"K:/scratch/gen_scripts/run_kdiba_gor01_one_2006-6-08_14-26-15",
        r"K:/scratch/gen_scripts/run_kdiba_gor01_two_2006-6-09_1-22-43",
    ]
    names = _session_tab_display_names(paths)
    assert names[paths[0]] == "gor01_one_2006-6-08_14-26-15"
    assert names[paths[1]] == "gor01_two_2006-6-09_1-22-43"


def test_session_tab_display_names_keeps_full_on_collision():
    # Same stripped suffix from different prefixes would collide; force collision
    # by using identical folder names under different parents (can't strip uniquely).
    paths = [
        r"K:/a/run_kdiba_same_session",
        r"K:/b/run_kdiba_same_session",
    ]
    names = _session_tab_display_names(paths)
    assert names[paths[0]] == "run_kdiba_same_session"
    assert names[paths[1]] == "run_kdiba_same_session"


def _recent_logs_row(parent: str, name: str, log_path: str, mtime=None, size: int = 1000) -> dict:
    return {
        "parent_path": parent,
        "name": name,
        "last_modified": _fresh_mtime() if mtime is None else mtime,
        "size": size,
        "log_path": log_path,
    }


def _status_row(parent: str, status: str = "finished") -> dict:
    return {
        "session": Path(parent).name,
        "status": status,
        "parent_path": parent,
        "log_path": None,
        "log_name": None,
        "log_size": None,
        "log_mtime": _fresh_mtime(),
        "out_path": None,
        "out_size": None,
        "err_path": None,
        "err_size": None,
        "age_hours": 0.1,
    }


def test_count_running_jobs_for_session_logs(monkeypatch):
    parent = r"K:/scratch/gen_scripts/run_kdiba_gor01_one_2006-6-08_14-26-15"
    path_run = f"{parent}/debug_run.log"
    path_done = f"{parent}/debug_done.log"
    texts = {path_run: _RUNNING_LOG, path_done: _COMPLETED_LOG}

    def fake_read(path, n_lines=0):
        return texts[str(path)]

    monkeypatch.setattr(
        "phoglobushelpers.gen_scripts_log_status.read_log_tail", fake_read
    )
    session_logs = pd.DataFrame(
        [
            _recent_logs_row(parent, "debug_run.log", path_run),
            _recent_logs_row(parent, "debug_done.log", path_done),
        ]
    )
    assert _count_running_jobs_for_session_logs(session_logs, {}, datetime.now(timezone.utc)) == 1


def test_build_html_finished_with_running_shows_plus_n(monkeypatch):
    parent = r"K:/scratch/gen_scripts/run_kdiba_gor01_one_2006-6-08_14-26-15"
    log_path = f"{parent}/debug_run.log"
    monkeypatch.setattr(
        "phoglobushelpers.gen_scripts_log_status.read_log_tail",
        lambda path, n_lines=0: _RUNNING_LOG,
    )
    recent_logs = pd.DataFrame([_recent_logs_row(parent, "debug_run.log", log_path)])
    status_df = pd.DataFrame([_status_row(parent, "finished")])
    html_fragment = _build_session_log_browser_html(recent_logs, status_df, n_tail_lines=0)
    assert "[+1] gor01_one_2006-6-08_14-26-15" in html_fragment
    assert "[finished]" not in html_fragment


def test_build_html_finished_all_completed_keeps_finished(monkeypatch):
    parent = r"K:/scratch/gen_scripts/run_kdiba_gor01_one_2006-6-08_14-26-15"
    log_path = f"{parent}/debug_done.log"
    monkeypatch.setattr(
        "phoglobushelpers.gen_scripts_log_status.read_log_tail",
        lambda path, n_lines=0: _COMPLETED_LOG,
    )
    recent_logs = pd.DataFrame([_recent_logs_row(parent, "debug_done.log", log_path)])
    status_df = pd.DataFrame([_status_row(parent, "finished")])
    html_fragment = _build_session_log_browser_html(recent_logs, status_df, n_tail_lines=0)
    assert "[finished] gor01_one_2006-6-08_14-26-15" in html_fragment
    assert "[+1]" not in html_fragment


def test_build_html_collision_keeps_full_session_names(monkeypatch):
    parent_a = r"K:/a/run_kdiba_same_session"
    parent_b = r"K:/b/run_kdiba_same_session"
    monkeypatch.setattr(
        "phoglobushelpers.gen_scripts_log_status.read_log_tail",
        lambda path, n_lines=0: _COMPLETED_LOG,
    )
    recent_logs = pd.DataFrame(
        [
            _recent_logs_row(parent_a, "debug_a.log", f"{parent_a}/debug_a.log"),
            _recent_logs_row(parent_b, "debug_b.log", f"{parent_b}/debug_b.log"),
        ]
    )
    status_df = pd.DataFrame(
        [
            _status_row(parent_a, "finished"),
            _status_row(parent_b, "finished"),
        ]
    )
    html_fragment = _build_session_log_browser_html(recent_logs, status_df, n_tail_lines=0)
    assert "[finished] run_kdiba_same_session" in html_fragment
    assert html_fragment.count("run_kdiba_same_session") >= 2


def test_build_html_unique_sessions_omit_run_kdiba_prefix(monkeypatch):
    parent_a = r"K:/scratch/gen_scripts/run_kdiba_gor01_one_2006-6-08_14-26-15"
    parent_b = r"K:/scratch/gen_scripts/run_kdiba_gor01_two_2006-6-09_1-22-43"
    monkeypatch.setattr(
        "phoglobushelpers.gen_scripts_log_status.read_log_tail",
        lambda path, n_lines=0: _COMPLETED_LOG,
    )
    recent_logs = pd.DataFrame(
        [
            _recent_logs_row(parent_a, "debug_a.log", f"{parent_a}/debug_a.log"),
            _recent_logs_row(parent_b, "debug_b.log", f"{parent_b}/debug_b.log"),
        ]
    )
    status_df = pd.DataFrame(
        [
            _status_row(parent_a, "finished"),
            _status_row(parent_b, "finished"),
        ]
    )
    html_fragment = _build_session_log_browser_html(recent_logs, status_df, n_tail_lines=0)
    assert "[finished] gor01_one_2006-6-08_14-26-15" in html_fragment
    assert "[finished] gor01_two_2006-6-09_1-22-43" in html_fragment
    # Tab button labels should not include the stripped prefix.
    tab_section = html_fragment.split('class="tablist"')[1].split("</div>")[0]
    assert "run_kdiba_" not in tab_section
