"""Local gen_scripts run-log status helpers for K:/scratch/gen_scripts (and similar).

Lists newest .log/.err/.out per session folder, classifies Running/Finished/etc.,
and optionally shows an inline Jupyter Tab+Accordion browser: left-hand session
tabs, each with an expanded accordion section per recent log.
"""
from __future__ import annotations

import html
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Union

import pandas as pd
from IPython.display import HTML, display

from phoglobushelpers.compatibility_objects.Files import get_only_most_recent_log_files

# Static HTML browser: no ipywidgets / Textarea / MutationObserver (those steal VS Code focus).
_SESSION_LOG_BROWSER_SEQ = 0
_SESSION_LOG_BROWSER_CSS = """
.gen-scripts-log-browser {
  display: flex;
  flex-direction: row;
  align-items: stretch;
  width: max-content;
  min-width: 100%;
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
  overflow-y: auto;
  border-right: 1px solid var(--vscode-panel-border, var(--jp-border-color1, #ccc));
}
.gen-scripts-log-browser .tablist button {
  appearance: none;
  display: block;
  width: 100%;
  text-align: left;
  white-space: nowrap;
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
  overflow: auto;
  padding: 10px 14px;
}
.gen-scripts-log-browser .session-heading {
  font-weight: 600;
  margin: 0 0 4px 0;
  white-space: nowrap;
}
.gen-scripts-log-browser .session-meta {
  font-size: 0.85em;
  opacity: 0.8;
  margin: 0 0 10px 0;
  white-space: nowrap;
}
.gen-scripts-log-browser details {
  margin: 0 0 10px 0;
  border: 1px solid var(--vscode-panel-border, var(--jp-border-color1, #ddd));
}
.gen-scripts-log-browser summary {
  white-space: nowrap;
  cursor: pointer;
  padding: 8px 10px;
  font-weight: 600;
}
.gen-scripts-log-browser .log-path {
  font-size: 0.85em;
  opacity: 0.8;
  margin: 0 10px 6px;
  white-space: nowrap;
}
.gen-scripts-log-browser pre {
  white-space: pre;
  overflow: auto;
  max-height: 480px;
  width: max-content;
  min-width: 100%;
  margin: 0;
  padding: 8px 10px;
  font-family: var(--vscode-editor-font-family, var(--jp-code-font-family, ui-monospace, Consolas, monospace));
  font-size: 12px;
  line-height: 1.35;
  user-select: text;
}
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
    """Compact UTC timestamp for accordion titles; ``?`` when missing."""
    if log_mtime is None or (isinstance(log_mtime, float) and pd.isna(log_mtime)):
        return "?"
    try:
        ts = pd.to_datetime(log_mtime, utc=True)
        if pd.isna(ts):
            return "?"
        return ts.strftime("%Y-%m-%d %H:%M UTC")
    except (TypeError, ValueError):
        return "?"


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
    stealing VS Code editor focus. Titles are nowrap (wide screens).
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
            f'class="{active_cls.strip()}" role="tab" aria-selected="{str(is_active).lower()}">'
            f"{tab_label}</button>"
        )

        log_blocks = []
        for log_row in session_logs.itertuples(index=False):
            tail = read_log_tail(log_row.log_path, n_lines=n_tail_lines)
            mtime_str = _format_log_mtime(log_row.last_modified)
            summary = html.escape(
                f"{log_row.name}  ·  {mtime_str}  ·  size={log_row.size}"
            )
            log_blocks.append(
                "<details open>"
                f"<summary tabindex='-1'>{summary}</summary>"
                f"<div class='log-path'><code>{html.escape(str(log_row.log_path))}</code></div>"
                f"<pre>{html.escape(tail)}</pre>"
                "</details>"
            )
        ## END for log_row in session_logs.itertuples(index=False)....

        hidden_attr = "" if is_active else " hidden"
        panels.append(
            f'<div class="panel{active_cls}" data-session-idx="{session_i}" role="tabpanel"{hidden_attr}>'
            f'<div class="session-heading">[{html.escape(str(status))}] {html.escape(session_name)}</div>'
            f'<div class="session-meta">{html.escape(newest_str)}  ·  {n_shown} log(s)</div>'
            f"{''.join(log_blocks)}"
            "</div>"
        )
    ## END for session_i, parent_path in enumerate(session_order.index)....

    return (
        f"<style>{_SESSION_LOG_BROWSER_CSS}</style>"
        f'<div id="{browser_id}" class="gen-scripts-log-browser">'
        f'<div class="tablist" role="tablist">{"".join(tab_buttons)}</div>'
        f'<div class="panels">{"".join(panels)}</div>'
        "</div>"
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
        'root.addEventListener("mousedown", function (e) {'
        '  var btn = e.target.closest("button[data-session-idx]");'
        "  if (!btn || !root.contains(btn)) { return; }"
        "  e.preventDefault();"
        '  setSession(btn.getAttribute("data-session-idx"));'
        "});"
        'root.querySelectorAll("summary").forEach(function (el) {'
        '  el.addEventListener("mousedown", function (e) { e.preventDefault(); });'
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
