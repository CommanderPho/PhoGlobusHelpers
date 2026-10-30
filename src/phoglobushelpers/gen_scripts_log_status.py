"""Local gen_scripts run-log status helpers for K:/scratch/gen_scripts (and similar).

Lists newest .log/.err/.out per session folder, classifies Running/Finished/etc.,
and optionally shows an inline Jupyter Tab+Accordion browser: left-hand session
tabs, each with an expanded accordion section per recent log.
"""
from __future__ import annotations

import html
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Union

import pandas as pd
from IPython.display import HTML, display

from phoglobushelpers.compatibility_objects.Files import get_only_most_recent_log_files

# Static HTML browser: no ipywidgets / Textarea / MutationObserver (those steal VS Code focus).
_SESSION_LOG_BROWSER_SEQ = 0
_SESSION_LOG_BROWSER_CSS = """
.gen-scripts-log-browser-wrap {
  box-sizing: border-box;
  width: 0;
  min-width: 100%;
  max-width: 100%;
  overflow: hidden;
}
.gen-scripts-log-browser {
  display: flex;
  flex-direction: row;
  align-items: stretch;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  min-width: 0;
  overflow: hidden;
  max-height: 80vh;
  border: 1px solid var(--vscode-panel-border, var(--jp-border-color1, #ccc));
  background: var(--vscode-editor-background, var(--jp-layout-color0, transparent));
  color: var(--vscode-editor-foreground, var(--jp-ui-font-color1, inherit));
}
.gen-scripts-log-browser .tablist {
  display: flex;
  flex-direction: column;
  flex: 0 0 auto;
  width: max-content;
  max-width: min(28em, 40%);
  min-width: 0;
  overflow-x: hidden;
  overflow-y: auto;
  border-right: 1px solid var(--vscode-panel-border, var(--jp-border-color1, #ccc));
}
.gen-scripts-log-browser .tablist button {
  appearance: none;
  display: block;
  width: 100%;
  text-align: left;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  border: none;
  border-left: 3px solid transparent;
  background: transparent;
  color: inherit;
  padding: 8px 14px;
  cursor: pointer;
  font: inherit;
}
.gen-scripts-log-browser .tablist button.is-active {
  background: var(--vscode-list-activeSelectionBackground, var(--jp-layout-color2, #e8e8e8));
  border-left-color: var(--vscode-focusBorder, #007acc);
  font-weight: 600;
}
.gen-scripts-log-browser .panels {
  flex: 1 1 auto;
  min-width: 0;
  overflow-x: hidden;
  overflow-y: auto;
  padding: 10px 14px;
}
.gen-scripts-log-browser .panel {
  min-width: 0;
  max-width: 100%;
}
.gen-scripts-log-browser .session-heading {
  font-weight: 600;
  margin: 0 0 4px 0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.gen-scripts-log-browser .session-meta {
  font-size: 0.85em;
  opacity: 0.8;
  margin: 0 0 10px 0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.gen-scripts-log-browser details {
  margin: 0 0 10px 0;
  max-width: 100%;
  min-width: 0;
  overflow: hidden;
  border: 1px solid var(--vscode-panel-border, var(--jp-border-color1, #ddd));
}
.gen-scripts-log-browser summary {
  cursor: pointer;
  padding: 8px 10px;
  font-weight: 600;
  overflow: hidden;
  background: var(--vscode-sideBar-background, var(--jp-layout-color1, rgba(127,127,127,0.08)));
}
.gen-scripts-log-browser .log-summary-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 8px;
  min-width: 0;
}
.gen-scripts-log-browser .log-summary-main {
  min-width: 0;
  flex: 1 1 auto;
  overflow: hidden;
}
.gen-scripts-log-browser .log-summary-full {
  display: block;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.gen-scripts-log-browser .log-summary-keys {
  display: block;
  margin-top: 2px;
  font-weight: 500;
  font-size: 0.92em;
  opacity: 0.9;
  letter-spacing: 0.01em;
}
.gen-scripts-log-browser .log-summary-actions {
  flex: 0 0 auto;
  display: flex;
  gap: 6px;
  align-items: center;
}
.gen-scripts-log-browser .log-action {
  appearance: none;
  display: inline-block;
  box-sizing: border-box;
  font: inherit;
  font-size: 11px;
  font-weight: 600;
  line-height: 1.2;
  padding: 3px 8px;
  cursor: pointer;
  border: 1px solid var(--vscode-panel-border, var(--jp-border-color1, #ccc));
  border-radius: 3px;
  background: var(--vscode-button-secondaryBackground, var(--jp-layout-color2, rgba(127,127,127,0.12)));
  color: inherit;
  text-decoration: none;
  white-space: nowrap;
}
.gen-scripts-log-browser .log-action:hover {
  background: var(--vscode-button-secondaryHoverBackground, var(--jp-layout-color3, rgba(127,127,127,0.22)));
}
.gen-scripts-log-browser .log-action.is-copied {
  border-color: #3fb950;
}
.gen-scripts-log-browser textarea.log-full-text {
  display: none;
}
.gen-scripts-log-browser .log-path {
  font-size: 0.85em;
  opacity: 0.8;
  margin: 6px 10px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.gen-scripts-log-browser .log-scroll {
  max-height: 480px;
  max-width: 100%;
  overflow: auto;
  background: var(--vscode-terminal-background, #0d1117);
  color: var(--vscode-terminal-foreground, #e6edf3);
  box-shadow: inset 0 0 0 1px rgba(255,255,255,0.06);
  border-left: 3px solid var(--vscode-focusBorder, #3b82f6);
}
.gen-scripts-log-browser .log-scroll pre {
  white-space: pre;
  overflow: visible;
  max-height: none;
  width: max-content;
  min-width: 100%;
  margin: 0;
  padding: 10px 12px;
  background: transparent;
  color: inherit;
  font-family: var(--vscode-editor-font-family, var(--jp-code-font-family, ui-monospace, Consolas, monospace));
  font-size: 12px;
  line-height: 1.4;
  user-select: text;
}
.gen-scripts-log-browser .tok-error { color: #f85149; font-weight: 600; }
.gen-scripts-log-browser .tok-ok { color: #3fb950; font-weight: 600; }
.gen-scripts-log-browser .tok-info { color: #d2a8ff; }
.gen-scripts-log-browser .tok-ts { color: #7ee787; }
.gen-scripts-log-browser .tok-host { color: #79c0ff; }
.gen-scripts-log-browser .tok-path { color: #e3b341; }
.gen-scripts-log-browser .tok-str { color: #a5d6ff; }
.gen-scripts-log-browser .tok-sep { color: #8b949e; }
.gen-scripts-log-browser .tok-key { color: #ffa657; }
"""

PathLike = Union[str, Path]
DateLike = Union[str, date, datetime]

# Heuristic thresholds (local sync snapshot only).
_FINISHED_OUT_SIZE: int = 1800
_TINY_LOG_SIZE: int = 5000
_LARGE_LOG_SIZE: int = 5000
_RUNNING_AGE_HOURS: float = 1.0
_EARLY_AGE_HOURS: float = 6.0


def list_local_gen_scripts_files(root: PathLike) -> pd.DataFrame:
    """Scan ``root/run_*/*`` for .log/.err/.out into a Globus-like file DataFrame.

    Columns: type, parent_path, name, last_modified, size
    """
    root_path = Path(root)
    rows = []
    for p in root_path.glob("run_*/*"):
        if p.suffix.lower() in {".log", ".err", ".out"} and p.is_file():
            st = p.stat()
            rows.append({
                "type": "file",
                "parent_path": str(p.parent),
                "name": p.name,
                "last_modified": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc),
                "size": st.st_size,
            })
    ## END for p in root_path.glob("run_*/*")....

    if not rows:
        return pd.DataFrame(columns=["type", "parent_path", "name", "last_modified", "size"])
    return pd.DataFrame(rows)


def classify_session_status(
    log_size: Optional[float],
    out_size: Optional[float],
    err_size: Optional[float],
    age_hours: Optional[float],
    err_has_traceback: bool = False,
) -> str:
    """Classify session from newest companion file sizes / age (local snapshot)."""
    log_size = float(log_size or 0)
    out_size = float(out_size or 0)
    err_size = float(err_size or 0)
    age_hours = float(age_hours) if age_hours is not None else float("inf")

    if err_has_traceback or err_size > 0:
        return "failed"
    if out_size >= _FINISHED_OUT_SIZE:
        return "finished"
    if log_size >= _LARGE_LOG_SIZE and age_hours <= _RUNNING_AGE_HOURS:
        return "running"
    if log_size < _TINY_LOG_SIZE and age_hours < _EARLY_AGE_HOURS:
        return "early_or_stuck"
    return "stale"


def _err_has_traceback(err_path: Optional[Path]) -> bool:
    if err_path is None or not err_path.is_file():
        return False
    try:
        # Only need a small sample for Traceback detection.
        text = err_path.read_text(errors="replace")[:200_000]
        return "Traceback" in text
    except OSError:
        return False


def _format_log_mtime(log_mtime) -> str:
    """Compact local 12-hour timestamp for accordion titles; ``?`` when missing."""
    if log_mtime is None or (isinstance(log_mtime, float) and pd.isna(log_mtime)):
        return "?"
    try:
        ts = pd.to_datetime(log_mtime, utc=True)
        if pd.isna(ts):
            return "?"
        local_dt = ts.to_pydatetime().astimezone()
        hour_12 = local_dt.hour % 12 or 12
        ampm = "AM" if local_dt.hour < 12 else "PM"
        return f"{local_dt.strftime('%Y-%m-%d')} {hour_12}:{local_dt.strftime('%M')} {ampm}"
    except (TypeError, ValueError, OSError, OverflowError):
        return "?"


_LOG_QCLU_RE = re.compile(r"qclu_\d+", re.IGNORECASE)
_LOG_TBIN_RE = re.compile(r"tbin_(\d+ms)", re.IGNORECASE)
_LOG_KIND_AFTER_TBIN_RE = re.compile(r"tbin_\d+ms[_-]([A-Za-z]+)", re.IGNORECASE)
_LOG_KIND_FALLBACK_RE = re.compile(r"_(Figure|Final|Continued)(?:[-.]|$)", re.IGNORECASE)


def _parse_log_filename_keys(name: str) -> list[str]:
    """Pull ``qclu_*``, tbin duration, and Figure/Final/Continued from a log filename."""
    stem = Path(str(name)).name
    keys: list[str] = []
    qclu = _LOG_QCLU_RE.search(stem)
    if qclu:
        keys.append(qclu.group(0))
    tbin = _LOG_TBIN_RE.search(stem)
    if tbin:
        keys.append(tbin.group(1))
    kind_match = _LOG_KIND_AFTER_TBIN_RE.search(stem) or _LOG_KIND_FALLBACK_RE.search(stem)
    if kind_match:
        keys.append(kind_match.group(1))
    return keys


def _editor_open_uris(path: PathLike) -> tuple[str, str, str]:
    """Return ``(file_uri, vscode_uri, cursor_uri)`` for opening ``path`` in an editor tab."""
    p = Path(path)
    try:
        resolved = p.resolve()
    except OSError:
        resolved = p
    posix = resolved.as_posix()
    try:
        file_uri = resolved.as_uri()
    except ValueError:
        file_uri = f"file:///{posix}"
    return file_uri, f"vscode://file/{posix}", f"cursor://file/{posix}"


_LOG_HIGHLIGHT_RE = re.compile(
    r"(?P<error>Traceback \(most recent call last\):|(?<![\w.])(?:AssertionError|FileNotFoundError|ValueError|TypeError|RuntimeError|KeyError|Exception|Error)(?![\w.]))"
    r"|(?P<ok>\bALL DONE!?)"
    r"|(?P<done>^[ \t]*done\.?[ \t]*$)"
    r"|(?P<info>\[[A-Z][A-Z_ ]*\])"
    r"|(?P<ts>\d{4}-\d{2}-\d{2}[ T_]\d{2}[:\-]\d{2}[:\-]\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?)"
    r"|(?P<host>\b[a-z0-9-]+(?:\.[a-z0-9-]+){2,}\b)"
    r"|(?P<path>(?:/[^\s:'\"]+)|(?:[A-Za-z]:[\\/][^\s'\"]+))"
    r"|(?P<str>\"[^\"\n]*\"|'[^'\n]*')"
    r"|(?P<sep>[=]{6,}|[-]{6,})"
    r"|(?P<key>\b[A-Za-z_][\w.]*(?=\s*:))",
    flags=re.MULTILINE,
)
_LOG_TOKEN_CLASS = {
    "error": "tok-error",
    "ok": "tok-ok",
    "done": "tok-ok",
    "info": "tok-info",
    "ts": "tok-ts",
    "host": "tok-host",
    "path": "tok-path",
    "str": "tok-str",
    "sep": "tok-sep",
    "key": "tok-key",
}


def _highlight_log_html(text: str) -> str:
    """Escape log text and wrap timestamps/levels/paths/errors in token spans."""
    parts = []
    pos = 0
    for match in _LOG_HIGHLIGHT_RE.finditer(text):
        if match.start() > pos:
            parts.append(html.escape(text[pos:match.start()]))
        cls = _LOG_TOKEN_CLASS.get(match.lastgroup or "", "")
        escaped = html.escape(match.group())
        if cls:
            parts.append(f'<span class="{cls}">{escaped}</span>')
        else:
            parts.append(escaped)
        pos = match.end()
    ## END for match in _LOG_HIGHLIGHT_RE.finditer(text)....

    parts.append(html.escape(text[pos:]))
    return "".join(parts)


def _coerce_utc_datetime(value: Optional[DateLike]) -> Optional[datetime]:
    """Normalize date/datetime/str to timezone-aware UTC datetime (or None)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    ts = pd.to_datetime(value, utc=True)
    if pd.isna(ts):
        return None
    return ts.to_pydatetime()


def resolve_cutoff_date(
    cutoff_date: Optional[DateLike] = None,
    max_num_day_ago: Optional[int] = 4,
    now: Optional[datetime] = None,
) -> Optional[datetime]:
    """Resolve cutoff: explicit ``cutoff_date``, else ``now - max_num_day_ago``, else None."""
    resolved = _coerce_utc_datetime(cutoff_date)
    if resolved is not None:
        return resolved
    if max_num_day_ago is None:
        return None
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return now - timedelta(days=int(max_num_day_ago))


def apply_remote_mtime_overrides(
    local_file_df: pd.DataFrame,
    override_df: pd.DataFrame,
) -> pd.DataFrame:
    """Overlay Globus ``last_modified`` onto local rows matched by session folder + name.

    Match key is ``(Path(parent_path).name, name)`` so remote paths like
    ``/umms-kdiba/.../gen_scripts/run_X`` align with local ``K:/scratch/gen_scripts/run_X``.

    Adds boolean column ``_mtime_overridden``. Unmatched local rows keep their
    filesystem ``last_modified``. Local ``parent_path`` / ``size`` are preserved.
    """
    if local_file_df is None or local_file_df.empty:
        cols = ["type", "parent_path", "name", "last_modified", "size", "_mtime_overridden"]
        if local_file_df is None:
            return pd.DataFrame(columns=cols)
        out = local_file_df.copy()
        out["_mtime_overridden"] = pd.Series(dtype=bool)
        return out

    out = local_file_df.copy()
    out["_mtime_overridden"] = False
    if override_df is None or override_df.empty:
        return out

    override = override_df.copy()
    override["session"] = override["parent_path"].map(lambda p: Path(str(p)).name)
    override["name"] = override["name"].astype(str)
    override["last_modified"] = pd.to_datetime(override["last_modified"], utc=True)
    # Last duplicate wins if override has repeated (session, name).
    mtime_map = (
        override.drop_duplicates(subset=["session", "name"], keep="last")
        .set_index(["session", "name"])["last_modified"]
    )

    out["session"] = out["parent_path"].map(lambda p: Path(str(p)).name)
    out["name"] = out["name"].astype(str)
    keys = list(zip(out["session"], out["name"]))
    mapped = pd.Series(keys, index=out.index).map(mtime_map)
    hit = mapped.notna()
    out.loc[hit, "last_modified"] = mapped.loc[hit]
    out.loc[hit, "_mtime_overridden"] = True
    out = out.drop(columns=["session"])
    return out


def select_recent_logs_by_session(
    file_df: pd.DataFrame,
    most_recent_n_logs: int = 3,
    cutoff_date: Optional[DateLike] = None,
) -> pd.DataFrame:
    """Per-session newest ``.log`` rows (up to ``most_recent_n_logs``) after ``cutoff_date``.

    Returns columns: parent_path, name, last_modified, size, log_path
    ordered by parent_path then last_modified descending.
    """
    empty_cols = ["parent_path", "name", "last_modified", "size", "log_path"]
    if file_df is None or file_df.empty or most_recent_n_logs <= 0:
        return pd.DataFrame(columns=empty_cols)

    logs = file_df[file_df["name"].astype(str).str.endswith(".log")].copy()
    if logs.empty:
        return pd.DataFrame(columns=empty_cols)

    logs["last_modified"] = pd.to_datetime(logs["last_modified"], utc=True)
    cutoff = _coerce_utc_datetime(cutoff_date)
    if cutoff is not None:
        cutoff_ts = pd.Timestamp(cutoff)
        if cutoff_ts.tzinfo is None:
            cutoff_ts = cutoff_ts.tz_localize("UTC")
        logs = logs[logs["last_modified"] > cutoff_ts]

    if logs.empty:
        return pd.DataFrame(columns=empty_cols)

    recent = (
        logs.sort_values("last_modified", ascending=False)
        .groupby("parent_path", group_keys=False)
        .head(most_recent_n_logs)
    )
    recent = recent.copy()
    recent["log_path"] = [
        str(Path(pp) / nm) for pp, nm in zip(recent["parent_path"], recent["name"])
    ]
    return recent.sort_values(
        by=["parent_path", "last_modified"], ascending=[True, False]
    ).reset_index(drop=True)


def read_log_tail(path: PathLike, n_lines: int = 0) -> str:
    """Return log text (UTF-8, replace errors).

    ``n_lines <= 0`` returns the entire file; a positive value returns the last N lines.
    """
    p = Path(path)
    if not p.is_file():
        return f"(missing: {p})"
    try:
        lines = p.read_text(errors="replace").splitlines(True)
    except OSError as exc:
        return f"(unreadable: {p}: {exc})"
    if n_lines <= 0:
        return "".join(lines)
    return "".join(lines[-n_lines:])


def build_session_status_df(
    file_df: pd.DataFrame,
    n_most_recent_files: int = 1,
    now: Optional[datetime] = None,
) -> pd.DataFrame:
    """One row per session (parent_path) with status and newest log/out/err metadata."""
    if file_df is None or file_df.empty:
        return pd.DataFrame(
            columns=[
                "session", "status", "parent_path", "log_path", "log_name",
                "log_size", "log_mtime", "out_path", "out_size", "err_path",
                "err_size", "age_hours",
            ]
        )

    recent = get_only_most_recent_log_files(
        log_file_df=file_df, n_most_recent_files=n_most_recent_files
    )
    empty_cols = [
        "session", "status", "parent_path", "log_path", "log_name",
        "log_size", "log_mtime", "out_path", "out_size", "err_path",
        "err_size", "age_hours",
    ]
    if recent.empty:
        return pd.DataFrame(columns=empty_cols)

    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    rows = []
    for parent_path, group in recent.groupby("parent_path", sort=False):
        log_rows = group[group["name"].str.endswith(".log")]
        out_rows = group[group["name"].str.endswith(".out")]
        err_rows = group[group["name"].str.endswith(".err")]

        log_row = log_rows.iloc[0] if not log_rows.empty else None
        out_row = out_rows.iloc[0] if not out_rows.empty else None
        err_row = err_rows.iloc[0] if not err_rows.empty else None

        log_path = Path(parent_path) / log_row["name"] if log_row is not None else None
        out_path = Path(parent_path) / out_row["name"] if out_row is not None else None
        err_path = Path(parent_path) / err_row["name"] if err_row is not None else None

        log_size = int(log_row["size"]) if log_row is not None else None
        out_size = int(out_row["size"]) if out_row is not None else None
        err_size = int(err_row["size"]) if err_row is not None else None
        log_mtime = pd.to_datetime(log_row["last_modified"]) if log_row is not None else None
        if log_mtime is not None and getattr(log_mtime, "tzinfo", None) is None:
            log_mtime = log_mtime.tz_localize("UTC")

        age_hours = None
        if log_mtime is not None:
            age_hours = (now - log_mtime.to_pydatetime()).total_seconds() / 3600.0

        status = classify_session_status(
            log_size=log_size,
            out_size=out_size,
            err_size=err_size,
            age_hours=age_hours,
            err_has_traceback=_err_has_traceback(err_path),
        )
        session = Path(parent_path).name
        rows.append({
            "session": session,
            "status": status,
            "parent_path": parent_path,
            "log_path": str(log_path) if log_path is not None else None,
            "log_name": log_row["name"] if log_row is not None else None,
            "log_size": log_size,
            "log_mtime": log_mtime,
            "out_path": str(out_path) if out_path is not None else None,
            "out_size": out_size,
            "err_path": str(err_path) if err_path is not None else None,
            "err_size": err_size,
            "age_hours": age_hours,
        })
    ## END for parent_path, group in recent.groupby("parent_path", sort=False)....

    status_df = pd.DataFrame(rows)
    if status_df.empty:
        return status_df
    return status_df.sort_values(by=["log_mtime"], ascending=False, na_position="last").reset_index(drop=True)


def _build_session_log_browser_html(
    recent_logs: pd.DataFrame,
    status_df: pd.DataFrame,
    n_tail_lines: int,
) -> str:
    """Left-hand session tabs + expanded ``<details>`` accordion per log.

    Static HTML/CSS/JS only. ``mousedown`` preventDefault keeps clicks from
    stealing VS Code editor focus. The widget is capped to the notebook cell
    / viewport so log ``<pre>`` lines scroll inside ``.log-scroll``.
    """
    global _SESSION_LOG_BROWSER_SEQ
    _SESSION_LOG_BROWSER_SEQ += 1
    browser_id = f"gen-scripts-log-browser-{_SESSION_LOG_BROWSER_SEQ}"

    session_order = (
        recent_logs.groupby("parent_path", sort=False)["last_modified"]
        .max()
        .sort_values(ascending=False)
    )
    status_by_parent = (
        status_df.set_index("parent_path")["status"].to_dict()
        if not status_df.empty
        else {}
    )

    tab_buttons = []
    panels = []
    for session_i, parent_path in enumerate(session_order.index):
        session_logs = recent_logs[recent_logs["parent_path"] == parent_path]
        session_name = Path(parent_path).name
        status = status_by_parent.get(parent_path, "?")
        newest_str = _format_log_mtime(session_order.loc[parent_path])
        n_shown = len(session_logs)
        is_active = session_i == 0
        active_cls = " is-active" if is_active else ""
        tab_label = html.escape(f"[{status}] {session_name}")
        tab_buttons.append(
            f'<button type="button" tabindex="-1" data-session-idx="{session_i}" '
            f'class="{active_cls.strip()}" role="tab" aria-selected="{str(is_active).lower()}" '
            f'title="{tab_label}">{tab_label}</button>'
        )

        log_blocks = []
        for log_row in session_logs.itertuples(index=False):
            full_text = read_log_tail(log_row.log_path, n_lines=0)
            display_text = (
                full_text
                if n_tail_lines <= 0
                else "".join(full_text.splitlines(True)[-n_tail_lines:])
            )
            mtime_str = _format_log_mtime(log_row.last_modified)
            keys = _parse_log_filename_keys(log_row.name)
            keys_str = "  ·  ".join(keys)
            summary_full = html.escape(
                f"{log_row.name}  ·  {mtime_str}  ·  size={log_row.size}"
            )
            title_plain = f"{log_row.name}  ·  {mtime_str}  ·  size={log_row.size}"
            if keys_str:
                title_plain = f"{log_row.name}  ·  {keys_str}  ·  {mtime_str}  ·  size={log_row.size}"
            keys_html = (
                f"<span class='log-summary-keys'>{html.escape(keys_str)}</span>"
                if keys_str
                else ""
            )
            file_uri, vscode_uri, cursor_uri = _editor_open_uris(log_row.log_path)
            full_copy_html = (
                ""
                if n_tail_lines <= 0
                else (
                    f"<textarea class='log-full-text' hidden readonly>"
                    f"{html.escape(full_text)}</textarea>"
                )
            )
            actions_html = (
                "<span class='log-summary-actions'>"
                f"<a class='log-action' data-open-editor tabindex='-1' "
                f"href='{html.escape(file_uri, quote=True)}' "
                f"data-vscode-uri='{html.escape(vscode_uri, quote=True)}' "
                f"data-cursor-uri='{html.escape(cursor_uri, quote=True)}' "
                f"title='Open in editor'>Open</a>"
                "<button type='button' class='log-action' data-copy-log tabindex='-1' "
                "title='Copy log to clipboard'>Copy</button>"
                "</span>"
            )
            log_path_html = html.escape(str(log_row.log_path))
            log_blocks.append(
                "<details open>"
                f"<summary tabindex='-1' title='{html.escape(title_plain)}'>"
                f"<div class='log-summary-head'>"
                f"<div class='log-summary-main'>"
                f"<span class='log-summary-full'>{summary_full}</span>"
                f"{keys_html}"
                f"</div>"
                f"{actions_html}"
                f"</div>"
                "</summary>"
                f"{full_copy_html}"
                f"<div class='log-path' title='{log_path_html}'><code>{log_path_html}</code></div>"
                f"<div class='log-scroll'><pre>{_highlight_log_html(display_text)}</pre></div>"
                "</details>"
            )
        ## END for log_row in session_logs.itertuples(index=False)....

        hidden_attr = "" if is_active else " hidden"
        heading = f"[{html.escape(str(status))}] {html.escape(session_name)}"
        panels.append(
            f'<div class="panel{active_cls}" data-session-idx="{session_i}" role="tabpanel"{hidden_attr}>'
            f'<div class="session-heading" title="{heading}">{heading}</div>'
            f'<div class="session-meta">{html.escape(newest_str)}  ·  {n_shown} log(s)</div>'
            f"{''.join(log_blocks)}"
            "</div>"
        )
    ## END for session_i, parent_path in enumerate(session_order.index)....

    return (
        f"<style>{_SESSION_LOG_BROWSER_CSS}</style>"
        f'<div class="gen-scripts-log-browser-wrap">'
        f'<div id="{browser_id}" class="gen-scripts-log-browser">'
        f'<div class="tablist" role="tablist">{"".join(tab_buttons)}</div>'
        f'<div class="panels">{"".join(panels)}</div>'
        "</div></div>"
        "<script>(function () {"
        f'var root = document.getElementById("{browser_id}");'
        "if (!root) { return; }"
        "function setSession(idx) {"
        '  root.querySelectorAll("[data-session-idx]").forEach(function (el) {'
        '    var on = el.getAttribute("data-session-idx") === idx;'
        '    el.classList.toggle("is-active", on);'
        '    if (el.tagName === "BUTTON") {'
        '      el.setAttribute("aria-selected", on ? "true" : "false");'
        "    } else {"
        "      el.hidden = !on;"
        "    }"
        "  });"
        "}"
        "function fitWidth() {"
        "  var cap = Math.max(640, (window.innerWidth || 1024) - 48);"
        "  root.style.maxWidth = cap + 'px';"
        "  var wrap = root.parentElement;"
        "  if (wrap) { wrap.style.maxWidth = cap + 'px'; }"
        "}"
        "fitWidth();"
        "window.addEventListener('resize', fitWidth);"
        'root.addEventListener("mousedown", function (e) {'
        '  var btn = e.target.closest("button[data-session-idx]");'
        "  if (!btn || !root.contains(btn)) { return; }"
        "  e.preventDefault();"
        '  setSession(btn.getAttribute("data-session-idx"));'
        "});"
        'root.querySelectorAll("summary").forEach(function (el) {'
        '  el.addEventListener("mousedown", function (e) {'
        '    if (e.target.closest(".log-action")) { return; }'
        "    e.preventDefault();"
        "  });"
        "});"
        'root.addEventListener("click", function (e) {'
        '  var action = e.target.closest(".log-action");'
        "  if (!action || !root.contains(action)) { return; }"
        "  e.stopPropagation();"
        '  if (action.hasAttribute("data-copy-log")) {'
        "    e.preventDefault();"
        "    var details = action.closest('details');"
        "    var src = details && (details.querySelector('textarea.log-full-text') || details.querySelector('.log-scroll pre'));"
        "    var text = '';"
        "    if (src) {"
        "      text = (src.tagName === 'TEXTAREA') ? (src.value || '') : (src.textContent || '');"
        "    }"
        "    function setCopyLabel(ok) {"
        "      action.textContent = ok ? 'Copied' : 'Copy failed';"
        "      action.classList.toggle('is-copied', !!ok);"
        "      setTimeout(function () {"
        "        action.textContent = 'Copy';"
        "        action.classList.remove('is-copied');"
        "      }, 1400);"
        "    }"
        "    function fallbackCopy() {"
        "      var ta = document.createElement('textarea');"
        "      ta.value = text;"
        "      ta.setAttribute('readonly', '');"
        "      ta.style.position = 'fixed';"
        "      ta.style.left = '-9999px';"
        "      document.body.appendChild(ta);"
        "      ta.select();"
        "      var ok = false;"
        "      try { ok = document.execCommand('copy'); } catch (err) {}"
        "      document.body.removeChild(ta);"
        "      setCopyLabel(ok);"
        "    }"
        "    if (navigator.clipboard && navigator.clipboard.writeText) {"
        "      navigator.clipboard.writeText(text).then(function () { setCopyLabel(true); }, fallbackCopy);"
        "    } else {"
        "      fallbackCopy();"
        "    }"
        "    return;"
        "  }"
        '  if (action.hasAttribute("data-open-editor")) {'
        "    e.preventDefault();"
        "    var uris = [action.getAttribute('data-vscode-uri'), action.getAttribute('data-cursor-uri'), action.getAttribute('href')];"
        "    var i;"
        "    for (i = 0; i < uris.length; i++) {"
        "      if (uris[i]) { window.open(uris[i], '_blank'); break; }"
        "    }"
        "  }"
        "});"
        "})();</script>"
    )


def show_session_log_browser(root: PathLike = r"K:/scratch/gen_scripts", n_tail_lines: int = 0, most_recent_n_logs: int = 3, cutoff_date: Optional[DateLike] = None, max_num_day_ago: Optional[int] = 4, n_most_recent_files: int = 1, most_recent_only_log_file_df: Optional[pd.DataFrame] = None, **kwargs) -> pd.DataFrame:
    """Display status table + left-tab session browser of recent logs.

    Sessions are left-hand tabs (newest log mtime first). Each session tab
    contains one expanded accordion (``<details open>``) per ``.log`` (up to
    ``most_recent_n_logs`` files newer than ``cutoff_date``, default:
    ``max_num_day_ago`` days before now).

    Rendered as static HTML so the browser does not steal VS Code focus
    (no ipywidgets Tab/Accordion/Textarea, no MutationObserver).

    By default ``n_tail_lines=0`` shows each log in full; set a positive value to
    show only the last N lines.

    When ``most_recent_only_log_file_df`` is provided (e.g. Globus listing), its
    ``last_modified`` values overlay local filesystem mtimes matched by session
    folder + filename. Only matched rows are kept for status/selection so stale
    copy mtimes cannot win. Log contents still come from local paths under ``root``.

    Returns the session status DataFrame.
    """
    root_path = Path(root)
    resolved_cutoff = resolve_cutoff_date(
        cutoff_date=cutoff_date, max_num_day_ago=max_num_day_ago
    )
    file_df = list_local_gen_scripts_files(root_path)
    if most_recent_only_log_file_df is not None:
        file_df = apply_remote_mtime_overrides(file_df, most_recent_only_log_file_df)
        if not file_df.empty:
            file_df = file_df[file_df["_mtime_overridden"]].copy()
        file_df = file_df.drop(columns=["_mtime_overridden"], errors="ignore")
    status_df = build_session_status_df(file_df, n_most_recent_files=n_most_recent_files)

    recent_logs = select_recent_logs_by_session(
        file_df,
        most_recent_n_logs=most_recent_n_logs,
        cutoff_date=resolved_cutoff,
    )
    sessions_with_logs = set(recent_logs["parent_path"].unique()) if not recent_logs.empty else set()
    if sessions_with_logs:
        display_status = status_df[status_df["parent_path"].isin(sessions_with_logs)].copy()
    else:
        display_status = status_df.iloc[0:0].copy()
    display(display_status)

    if not recent_logs.empty:
        display(HTML(_build_session_log_browser_html(recent_logs, status_df, n_tail_lines)))
    else:
        cutoff_msg = (
            f" newer than {resolved_cutoff.isoformat()}"
            if resolved_cutoff is not None
            else ""
        )
        print(f"No .log files{cutoff_msg} found under {root_path}")
    return status_df
