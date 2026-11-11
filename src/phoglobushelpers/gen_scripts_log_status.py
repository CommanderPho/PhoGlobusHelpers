"""Local gen_scripts run-log status helpers for K:/scratch/gen_scripts (and similar).

Lists newest .log/.err/.out per session folder, classifies Running/Finished/etc.,
and optionally shows an inline Jupyter Tab+Accordion browser: left-hand session
tabs, each with an expanded accordion section per recent log.
"""
from __future__ import annotations

import html
import re
import tempfile
import webbrowser
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Sequence, Union
from zoneinfo import ZoneInfo

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
  border: 1px solid var(--vscode-panel-border, var(--jp-border-color1, #3c3c3c));
  background: var(--vscode-editor-background, var(--jp-layout-color0, #1e1e1e));
  color: var(--vscode-editor-foreground, var(--jp-ui-font-color1, #cccccc));
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
  border-right: 1px solid var(--vscode-panel-border, var(--jp-border-color1, #3c3c3c));
  background: var(--vscode-sideBar-background, var(--jp-layout-color1, #252526));
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
.gen-scripts-log-browser .tablist button:hover:not(.is-active) {
  background: #2a2d2e;
}
.gen-scripts-log-browser .tablist button.is-active {
  background: var(--vscode-list-activeSelectionBackground, var(--jp-layout-color2, #094771));
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
  border: 1px solid var(--vscode-panel-border, var(--jp-border-color1, #3c3c3c));
}
.gen-scripts-log-browser summary {
  cursor: pointer;
  padding: 8px 10px;
  font-weight: 600;
  overflow: hidden;
  background: var(--vscode-sideBar-background, var(--jp-layout-color1, #252526));
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
.gen-scripts-log-browser .log-summary-job {
  display: block;
  margin-top: 2px;
  font-weight: 500;
  font-size: 0.92em;
  letter-spacing: 0.01em;
}
.gen-scripts-log-browser .job-status-completed { color: #3fb950; font-weight: 600; }
.gen-scripts-log-browser .job-status-running { color: #79c0ff; font-weight: 600; }
.gen-scripts-log-browser .job-status-failed { color: #f85149; font-weight: 600; }
.gen-scripts-log-browser .job-status-unknown { opacity: 0.75; font-weight: 600; }
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
  border: 1px solid var(--vscode-panel-border, var(--jp-border-color1, #3c3c3c));
  border-radius: 3px;
  background: var(--vscode-button-secondaryBackground, var(--jp-layout-color2, #3a3d41));
  color: inherit;
  text-decoration: none;
  white-space: nowrap;
}
.gen-scripts-log-browser .log-action:hover {
  background: var(--vscode-button-secondaryHoverBackground, var(--jp-layout-color3, #45494e));
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


def _resolve_log_local_tz():
    """Great Lakes log lines are local wall clock without offsets.

    Prefer ``America/Detroit`` (needs ``tzdata`` on Windows). Fall back to fixed
    EST (UTC-5) when IANA zones are unavailable.
    """
    try:
        return ZoneInfo("America/Detroit")
    except Exception:
        return timezone(timedelta(hours=-5))


_LOG_LOCAL_TZ = _resolve_log_local_tz()


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


_SLURM_JOB_ID_RE = re.compile(r"-(\d+)\.out$", re.IGNORECASE)
_SLURM_DEBUG_LOG_RE = re.compile(
    r"(?P<log>debug_[^\s\"'/\\]+\.log)",
    re.IGNORECASE,
)
_SLURM_LOGGER_STRING_RE = re.compile(
    r'full_logger_string\s*=\s*"(?P<logger>[^"]+)"',
    re.IGNORECASE,
)
_BATCH_LOAD_START_RE = re.compile(
    r"\[TIMING START\]\s+batch_load_session\s+@\s+"
    r"(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})",
)
_LOG_LINE_TS_RE = re.compile(
    r"(?P<ts>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)"
)
_COMPLETED_RE = re.compile(
    r"finished run:\s*SessionBatchProgress\.COMPLETED|\bALL DONE!?",
    re.IGNORECASE,
)
_FAILED_PROGRESS_RE = re.compile(
    r"finished run:\s*SessionBatchProgress\.FAILED"
    r"|SessionBatchProgress\.FAILED",
    re.IGNORECASE,
)
_ERRORS_NONEMPTY_RE = re.compile(
    r"^errors:\s*(?!None\b|null\b|$)\S",
    re.IGNORECASE | re.MULTILINE,
)


def _slurm_job_id_from_name(name: str) -> Optional[str]:
    """Extract trailing Slurm job ID from ``slurm_*-{id}.out`` filename."""
    match = _SLURM_JOB_ID_RE.search(Path(str(name)).name)
    return match.group(1) if match else None


def _slurm_display_name_from_name(name: str) -> Optional[str]:
    """Strip ``slurm_`` prefix and ``-{id}.out`` suffix for OOD-style job Name."""
    stem = Path(str(name)).name
    if not stem.lower().startswith("slurm_") or not stem.lower().endswith(".out"):
        return None
    body = stem[len("slurm_") :]
    id_match = _SLURM_JOB_ID_RE.search(stem)
    if id_match:
        body = body[: -(len(id_match.group(0)))]
    return body or None


def _format_duration_hms(seconds: Optional[float]) -> Optional[str]:
    """Format non-negative seconds as ``HH:MM:SS`` (OOD Time Used style).

    Fractional seconds are truncated (floor), matching whole-second Slurm displays.
    """
    if seconds is None:
        return None
    total = int(max(0, float(seconds)))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _read_file_head(path: PathLike, max_bytes: int = 4096) -> str:
    """Read up to ``max_bytes`` from the start of a file (UTF-8, replace errors)."""
    p = Path(path)
    if not p.is_file():
        return ""
    try:
        with p.open("rb") as fh:
            raw = fh.read(max_bytes)
        return raw.decode("utf-8", errors="replace")
    except OSError:
        return ""


def _debug_log_stem(name: str) -> str:
    """Basename of a debug log without ``.log`` (match key for slurm headers)."""
    stem = Path(str(name)).name
    if stem.lower().endswith(".log"):
        return stem[:-4]
    return stem


def _match_keys_from_slurm_header(header: str) -> list[str]:
    """Extract debug-log match keys from a short ``slurm_*.out`` header sample."""
    keys: list[str] = []
    for match in _SLURM_DEBUG_LOG_RE.finditer(header):
        keys.append(_debug_log_stem(match.group("log")))
    logger_match = _SLURM_LOGGER_STRING_RE.search(header)
    if logger_match:
        logger = logger_match.group("logger").strip()
        # Headers often omit the ``debug_`` prefix in full_logger_string.
        if logger.lower().startswith("debug_"):
            keys.append(_debug_log_stem(logger if logger.lower().endswith(".log") else f"{logger}.log"))
        else:
            keys.append(f"debug_{logger}")
    # Preserve order, drop duplicates.
    seen: set[str] = set()
    unique: list[str] = []
    for key in keys:
        if key and key not in seen:
            seen.add(key)
            unique.append(key)
    return unique


def build_slurm_out_lookup(file_df: pd.DataFrame) -> dict[str, dict[str, dict]]:
    """Index ``slurm_*.out`` rows by ``parent_path`` then debug-log match key.

    Each value dict has: ``job_id``, ``job_name``, ``out_path``, ``out_mtime``,
    ``out_size``. Match keys come from the first ~4KB of each ``.out`` header
    (``final_log_file_path`` / ``full_logger_string``).
    """
    empty: dict[str, dict[str, dict]] = {}
    if file_df is None or file_df.empty:
        return empty

    outs = file_df[file_df["name"].astype(str).str.lower().str.endswith(".out")].copy()
    if outs.empty:
        return empty

    outs = outs[outs["name"].astype(str).str.lower().str.startswith("slurm_")]
    if outs.empty:
        return empty

    lookup: dict[str, dict[str, dict]] = {}
    for row in outs.itertuples(index=False):
        parent = str(row.parent_path)
        name = str(row.name)
        job_id = _slurm_job_id_from_name(name)
        if not job_id:
            continue
        out_path = Path(parent) / name
        header = _read_file_head(out_path, max_bytes=4096)
        match_keys = _match_keys_from_slurm_header(header)
        if not match_keys:
            # Fallback: kind token from slurm name (Clean/Continued/Final/Figure).
            kind_keys = _parse_log_filename_keys(name)
            if kind_keys:
                match_keys = [f"__kind__{kind_keys[-1].lower()}"]
        info = {
            "job_id": job_id,
            "job_name": _slurm_display_name_from_name(name),
            "out_path": str(out_path),
            "out_mtime": pd.to_datetime(row.last_modified, utc=True),
            "out_size": int(row.size) if row.size is not None else None,
        }
        session_map = lookup.setdefault(parent, {})
        for key in match_keys:
            session_map[key] = info
    ## END for row in outs.itertuples(index=False)....

    return lookup


def lookup_slurm_info_for_log(
    slurm_lookup: dict[str, dict[str, dict]],
    parent_path: str,
    log_name: str,
) -> Optional[dict]:
    """Resolve slurm companion info for a debug log via header match, then kind."""
    session_map = slurm_lookup.get(str(parent_path)) or {}
    if not session_map:
        return None
    stem = _debug_log_stem(log_name)
    if stem in session_map:
        return session_map[stem]
    keys = _parse_log_filename_keys(log_name)
    if keys:
        kind_key = f"__kind__{keys[-1].lower()}"
        if kind_key in session_map:
            return session_map[kind_key]
    return None


def _parse_ts_to_utc(value: Optional[str]) -> Optional[datetime]:
    """Parse a log timestamp string to timezone-aware UTC datetime.

    Naive timestamps (no offset) are interpreted as Great Lakes local time
    (``America/Detroit``), then converted to UTC so they compare correctly with
    Globus/FS ``out_mtime`` values which are already UTC.
    """
    if not value:
        return None
    text = str(value).strip().replace(",", ".")
    try:
        ts = pd.to_datetime(text)
    except (TypeError, ValueError):
        return None
    if pd.isna(ts):
        return None
    dt = ts.to_pydatetime()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_LOG_LOCAL_TZ)
    return dt.astimezone(timezone.utc)


def parse_debug_log_job_info(
    log_text: str,
    *,
    log_name: str = "",
    log_mtime=None,
    slurm_info: Optional[dict] = None,
    now: Optional[datetime] = None,
) -> dict:
    """Extract job_id, job_name, job_status, and Time Used from debug log text.

    Status: Completed / Running / Failed / Unknown.
    Time Used: ``batch_load_session`` start → completion (or ``now`` if running);
    when ``slurm_info['out_mtime']`` is later than the log end, use it as the end
    (hybrid alignment with Slurm finish).
    """
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    job_id = (slurm_info or {}).get("job_id")
    job_name = (slurm_info or {}).get("job_name")
    out_mtime = _coerce_utc_datetime((slurm_info or {}).get("out_mtime"))

    has_traceback = "Traceback (most recent call last):" in (log_text or "")
    has_failed_progress = bool(_FAILED_PROGRESS_RE.search(log_text or ""))
    has_errors = bool(_ERRORS_NONEMPTY_RE.search(log_text or ""))
    has_completed = bool(_COMPLETED_RE.search(log_text or ""))

    # Explicit batch completion wins over mid-run Tracebacks (common in Figure jobs).
    if has_failed_progress or has_errors:
        job_status = "Failed"
    elif has_completed:
        job_status = "Completed"
    elif has_traceback:
        job_status = "Failed"
    else:
        age_hours = None
        coerced_mtime = _coerce_utc_datetime(log_mtime)
        if coerced_mtime is not None:
            age_hours = (now - coerced_mtime).total_seconds() / 3600.0
        if age_hours is not None and age_hours <= _RUNNING_AGE_HOURS:
            job_status = "Running"
        else:
            job_status = "Unknown"

    start = None
    start_match = _BATCH_LOAD_START_RE.search(log_text or "")
    if start_match:
        start = _parse_ts_to_utc(start_match.group("ts"))

    log_end = None
    # Prefer timestamps on / near completion markers (scan last portion + all matches).
    for match in _COMPLETED_RE.finditer(log_text or ""):
        window_start = max(0, match.start() - 200)
        window_end = min(len(log_text), match.end() + 200)
        window = log_text[window_start:window_end]
        for ts_match in _LOG_LINE_TS_RE.finditer(window):
            parsed = _parse_ts_to_utc(ts_match.group("ts"))
            if parsed is not None and (log_end is None or parsed > log_end):
                log_end = parsed

    if log_end is None and job_status in {"Completed", "Failed"}:
        # Last timestamp anywhere in the log (often the INFO lines after ALL DONE).
        for ts_match in _LOG_LINE_TS_RE.finditer(log_text or ""):
            parsed = _parse_ts_to_utc(ts_match.group("ts"))
            if parsed is not None and (log_end is None or parsed > log_end):
                log_end = parsed
        if log_end is None:
            log_end = _coerce_utc_datetime(log_mtime)

    time_used_seconds = None
    time_used = None
    if start is not None:
        if job_status == "Running":
            end = max(filter(None, [now, _coerce_utc_datetime(log_mtime)]), default=now)
        else:
            end = log_end
            if end is None:
                end = _coerce_utc_datetime(log_mtime)
            if out_mtime is not None and end is not None and out_mtime > end:
                end = out_mtime
            elif out_mtime is not None and end is None and out_mtime > start:
                end = out_mtime
        if end is not None and end >= start:
            time_used_seconds = (end - start).total_seconds()
            time_used = _format_duration_hms(time_used_seconds)

    return {
        "job_id": job_id,
        "job_name": job_name,
        "job_status": job_status,
        "time_used_seconds": time_used_seconds,
        "time_used": time_used,
        "start_time": start,
        "log_name": log_name or None,
    }


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


def _job_status_css_class(job_status: Optional[str]) -> str:
    """Map job_status label to a CSS class for accordion badges."""
    key = (job_status or "Unknown").strip().lower()
    return {
        "completed": "job-status-completed",
        "running": "job-status-running",
        "failed": "job-status-failed",
    }.get(key, "job-status-unknown")


def _format_job_summary_html(job_info: dict) -> str:
    """Render OOD-style ``job_id · name · Status · HH:MM:SS`` for accordion titles."""
    parts: list[str] = []
    if job_info.get("job_id"):
        parts.append(html.escape(str(job_info["job_id"])))
    if job_info.get("job_name"):
        parts.append(html.escape(str(job_info["job_name"])))
    status = job_info.get("job_status") or "Unknown"
    status_cls = _job_status_css_class(status)
    parts.append(f'<span class="{status_cls}">{html.escape(str(status))}</span>')
    if job_info.get("time_used"):
        parts.append(html.escape(str(job_info["time_used"])))
    if not parts:
        return ""
    return f"<span class='log-summary-job'>{'  ·  '.join(parts)}</span>"


_RUN_KDIBA_SESSION_PREFIX = "run_kdiba_"


def _session_tab_display_names(parent_paths: Sequence[str]) -> dict[str, str]:
    """Map parent paths to tab labels; strip ``run_kdiba_`` when stripped names stay unique."""
    full_names = {p: Path(p).name for p in parent_paths}
    stripped = {
        p: (name[len(_RUN_KDIBA_SESSION_PREFIX):] if name.startswith(_RUN_KDIBA_SESSION_PREFIX) else name)
        for p, name in full_names.items()
    }
    if len(set(stripped.values())) == len(stripped):
        return stripped
    return full_names


def _session_tab_status_token(heuristic_status: str, n_running_jobs: int) -> str:
    """Replace misleading ``finished`` with ``+N`` when any log job is still Running."""
    if heuristic_status == "finished" and n_running_jobs > 0:
        return f"+{n_running_jobs}"
    return heuristic_status


def _count_running_jobs_for_session_logs(
    session_logs: pd.DataFrame,
    slurm_lookup: Optional[dict] = None,
    now: Optional[datetime] = None,
) -> int:
    """Count logs in ``session_logs`` whose parsed ``job_status`` is ``Running``."""
    if session_logs is None or session_logs.empty:
        return 0
    if now is None:
        now = datetime.now(timezone.utc)
    lookup = slurm_lookup or {}
    n_running = 0
    for log_row in session_logs.itertuples(index=False):
        log_path = getattr(log_row, "log_path", None)
        if not log_path:
            continue
        text = read_log_tail(log_path, n_lines=0)
        if text.startswith("(missing:") or text.startswith("(unreadable:"):
            continue
        log_name = str(getattr(log_row, "name", "") or Path(log_path).name)
        parent_path = str(getattr(log_row, "parent_path", "") or "")
        slurm_info = lookup_slurm_info_for_log(lookup, parent_path, log_name)
        job_info = parse_debug_log_job_info(
            text,
            log_name=log_name,
            log_mtime=getattr(log_row, "last_modified", None),
            slurm_info=slurm_info,
            now=now,
        )
        if job_info.get("job_status") == "Running":
            n_running += 1
    ## END for log_row in session_logs.itertuples(index=False)...

    return n_running


def _build_session_log_browser_html(
    recent_logs: pd.DataFrame,
    status_df: pd.DataFrame,
    n_tail_lines: int,
    file_df: Optional[pd.DataFrame] = None,
) -> str:
    """Left-hand session tabs + expanded ``<details>`` accordion per log.

    Static HTML/CSS/JS only. ``mousedown`` preventDefault keeps clicks from
    stealing VS Code editor focus. The widget is capped to the notebook cell
    / viewport so log ``<pre>`` lines scroll inside ``.log-scroll``.

    When ``file_df`` is provided, each log summary includes Slurm job ID, status,
    and Time Used from ``parse_debug_log_job_info``.
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
    slurm_lookup = build_slurm_out_lookup(file_df) if file_df is not None else {}
    now = datetime.now(timezone.utc)
    display_names = _session_tab_display_names(list(session_order.index))

    tab_buttons = []
    panels = []
    for session_i, parent_path in enumerate(session_order.index):
        session_logs = recent_logs[recent_logs["parent_path"] == parent_path]
        status = status_by_parent.get(parent_path, "?")
        newest_str = _format_log_mtime(session_order.loc[parent_path])
        n_shown = len(session_logs)
        is_active = session_i == 0
        active_cls = " is-active" if is_active else ""
        n_running = _count_running_jobs_for_session_logs(session_logs, slurm_lookup, now)
        status_token = _session_tab_status_token(status, n_running)
        session_name = display_names[parent_path]
        tab_label = html.escape(f"[{status_token}] {session_name}")
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
            slurm_info = lookup_slurm_info_for_log(
                slurm_lookup, str(log_row.parent_path), str(log_row.name)
            )
            job_info = parse_debug_log_job_info(
                full_text,
                log_name=str(log_row.name),
                log_mtime=log_row.last_modified,
                slurm_info=slurm_info,
                now=now,
            )
            job_html = _format_job_summary_html(job_info)
            job_plain_parts = [
                p for p in (
                    job_info.get("job_id"),
                    job_info.get("job_name"),
                    job_info.get("job_status"),
                    job_info.get("time_used"),
                ) if p
            ]
            job_plain = "  ·  ".join(str(p) for p in job_plain_parts)
            summary_full = html.escape(
                f"{log_row.name}  ·  {mtime_str}  ·  size={log_row.size}"
            )
            title_plain = f"{log_row.name}  ·  {mtime_str}  ·  size={log_row.size}"
            if keys_str:
                title_plain = f"{log_row.name}  ·  {keys_str}  ·  {mtime_str}  ·  size={log_row.size}"
            if job_plain:
                title_plain = f"{title_plain}  ·  {job_plain}"
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
                f"{job_html}"
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
        heading = f"[{html.escape(str(status_token))}] {html.escape(session_name)}"
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


def _wrap_session_log_browser_document(html_fragment: str, *, title: str = "Session log browser") -> str:
    """Wrap an HTML fragment from ``_build_session_log_browser_html`` in a full document."""
    safe_title = html.escape(title)
    return (
        "<!DOCTYPE html>\n"
        '<html lang="en">\n'
        "<head>\n"
        '<meta charset="utf-8"/>\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1"/>\n'
        f"<title>{safe_title}</title>\n"
        "<style>"
        "html, body { margin: 0; padding: 0; min-height: 100%; "
        "background: #1e1e1e; color: #cccccc; }"
        "body { padding: 8px; box-sizing: border-box; }"
        "</style>\n"
        "</head>\n"
        f"<body>\n{html_fragment}\n</body>\n"
        "</html>\n"
    )


def _save_session_log_browser_html(
    html_fragment: str,
    *,
    path: Optional[PathLike] = None,
    title: str = "Session log browser",
) -> Path:
    """Write a standalone HTML file; default path is under the system temp dir."""
    if path is None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out = Path(tempfile.gettempdir()) / f"gen_scripts_log_browser_{stamp}.html"
    else:
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        _wrap_session_log_browser_document(html_fragment, title=title),
        encoding="utf-8",
    )
    return out


def _open_html_in_default_browser(path: PathLike) -> None:
    """Open a local HTML file in the system default browser."""
    resolved = Path(path).resolve()
    url = resolved.as_uri()
    try:
        webbrowser.open_new_tab(url)
    except webbrowser.Error:
        print("No browser available (likely headless server).")


def show_session_log_browser(
    root: PathLike = r"K:/scratch/gen_scripts",
    n_tail_lines: int = 0,
    most_recent_n_logs: int = 3,
    cutoff_date: Optional[DateLike] = None,
    max_num_day_ago: Optional[int] = 4,
    n_most_recent_files: int = 1,
    most_recent_only_log_file_df: Optional[pd.DataFrame] = None,
    *,
    open_in_browser: bool = False,
    display_in_notebook: bool = True,
    browser_html_path: Optional[PathLike] = None,
) -> pd.DataFrame:
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

    Set ``open_in_browser=True`` to write a standalone HTML file and open it in
    the system default browser. Use ``display_in_notebook=False`` with that to
    avoid embedding the widget in the notebook (recommended when browsing
    externally). Optionally pass ``browser_html_path`` for a stable output path;
    otherwise a timestamped file is written under the system temp directory.
    Prefer at least one of ``open_in_browser`` / ``display_in_notebook`` when
    logs are present.

    Returns the session status DataFrame (enriched with ``job_id``, ``job_status``,
    and ``time_used`` for the newest log per session when parseable).
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
    status_df = enrich_status_df_with_job_info(status_df, file_df)

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
        html_fragment = _build_session_log_browser_html(
            recent_logs, status_df, n_tail_lines, file_df=file_df
        )
        if open_in_browser:
            out_path = _save_session_log_browser_html(
                html_fragment, path=browser_html_path
            )
            print(f"Opening session log browser: {out_path}")
            _open_html_in_default_browser(out_path)
        if display_in_notebook:
            display(HTML(html_fragment))
    else:
        cutoff_msg = (
            f" newer than {resolved_cutoff.isoformat()}"
            if resolved_cutoff is not None
            else ""
        )
        print(f"No .log files{cutoff_msg} found under {root_path}")
    return status_df


def enrich_status_df_with_job_info(
    status_df: pd.DataFrame,
    file_df: Optional[pd.DataFrame] = None,
    now: Optional[datetime] = None,
) -> pd.DataFrame:
    """Add ``job_id``, ``job_status``, ``time_used`` from each session's newest log."""
    if status_df is None or status_df.empty:
        out = status_df.copy() if status_df is not None else pd.DataFrame()
        for col in ("job_id", "job_status", "time_used"):
            if col not in out.columns:
                out[col] = pd.Series(dtype=object)
        return out

    out = status_df.copy()
    for col in ("job_id", "job_status", "time_used"):
        if col not in out.columns:
            out[col] = None

    slurm_lookup = build_slurm_out_lookup(file_df) if file_df is not None else {}
    if now is None:
        now = datetime.now(timezone.utc)

    for idx, row in out.iterrows():
        log_path = row.get("log_path")
        log_name = row.get("log_name")
        if not log_path:
            continue
        text = read_log_tail(log_path, n_lines=0)
        if text.startswith("(missing:") or text.startswith("(unreadable:"):
            continue
        slurm_info = lookup_slurm_info_for_log(
            slurm_lookup, str(row.get("parent_path") or ""), str(log_name or Path(log_path).name)
        )
        job_info = parse_debug_log_job_info(
            text,
            log_name=str(log_name or Path(log_path).name),
            log_mtime=row.get("log_mtime"),
            slurm_info=slurm_info,
            now=now,
        )
        out.at[idx, "job_id"] = job_info.get("job_id")
        out.at[idx, "job_status"] = job_info.get("job_status")
        out.at[idx, "time_used"] = job_info.get("time_used")
    ## END for idx, row in out.iterrows()....

    return out
