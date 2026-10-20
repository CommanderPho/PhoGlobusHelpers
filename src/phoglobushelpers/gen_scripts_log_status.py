"""Local gen_scripts run-log status helpers for K:/scratch/gen_scripts (and similar).

Lists newest .log/.err/.out per session folder, classifies Running/Finished/etc.,
and optionally shows an inline Jupyter Accordion with recent log tails per session.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Union

import pandas as pd
from IPython.display import display

from phoglobushelpers.compatibility_objects.Files import get_only_most_recent_log_files

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


def read_log_tail(path: PathLike, n_lines: int = 80) -> str:
    """Return the last ``n_lines`` of a text file (UTF-8, replace errors)."""
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


def show_session_log_browser(
    root: PathLike = r"K:/scratch/gen_scripts",
    n_tail_lines: int = 80,
    most_recent_n_logs: int = 3,
    cutoff_date: Optional[DateLike] = None,
    max_num_day_ago: Optional[int] = 4,
    n_most_recent_files: int = 1,
) -> pd.DataFrame:
    """Display status table + Accordion of recent log tails (one section per session).

    Each session section shows up to ``most_recent_n_logs`` ``.log`` tails whose
    ``last_modified`` is newer than ``cutoff_date`` (default: ``max_num_day_ago``
    days before now). Sessions are ordered by newest log mtime.

    Returns the session status DataFrame. Requires ipywidgets in the Jupyter kernel.
    """
    import ipywidgets as widgets

    root_path = Path(root)
    resolved_cutoff = resolve_cutoff_date(
        cutoff_date=cutoff_date, max_num_day_ago=max_num_day_ago
    )
    file_df = list_local_gen_scripts_files(root_path)
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

    # One accordion entry per session; newest session first.
    children = []
    titles = []
    if not recent_logs.empty:
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
        for parent_path in session_order.index:
            session_logs = recent_logs[recent_logs["parent_path"] == parent_path]
            blocks = []
            for log_row in session_logs.itertuples(index=False):
                tail = read_log_tail(log_row.log_path, n_lines=n_tail_lines)
                blocks.append(
                    f"{log_row.name}\n"
                    f"size={log_row.size}  mtime={log_row.last_modified}\n"
                    f"{log_row.log_path}\n"
                    f"{'-' * 72}\n"
                    f"{tail}"
                )
            ## END for log_row in session_logs.itertuples(index=False)....

            n_shown = len(blocks)
            height_px = min(720, 280 + max(0, n_shown - 1) * 200)
            children.append(
                widgets.Textarea(
                    value=("\n\n" + ("=" * 72) + "\n\n").join(blocks),
                    layout=widgets.Layout(width="100%", height=f"{height_px}px"),
                    disabled=True,
                )
            )
            session_name = Path(parent_path).name
            status = status_by_parent.get(parent_path, "?")
            newest_mtime = session_order.loc[parent_path]
            mtime_str = _format_log_mtime(newest_mtime)
            titles.append(
                f"[{status}] {session_name}  ·  {mtime_str}  ·  {n_shown} log(s)"
            )
        ## END for parent_path in session_order.index....

    if children:
        accordion = widgets.Accordion(children=children)
        for i, title in enumerate(titles):
            accordion.set_title(i, title)
        ## END for i, title in enumerate(titles)....
        display(accordion)
    else:
        cutoff_msg = (
            f" newer than {resolved_cutoff.isoformat()}"
            if resolved_cutoff is not None
            else ""
        )
        print(f"No .log files{cutoff_msg} found under {root_path}")
    return status_df
