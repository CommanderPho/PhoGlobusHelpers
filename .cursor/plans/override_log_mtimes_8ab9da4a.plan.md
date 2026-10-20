---
name: Override log mtimes
overview: Add an optional Globus-sourced DataFrame to `show_session_log_browser` that overlays authoritative `last_modified` values onto the local file scan (matched by session folder + filename), so cutoff, ordering, and status age use remote times when local FS mtimes are wrong.
todos:
  - id: mtime-helper
    content: Add apply_remote_mtime_overrides(local_file_df, override_df) matching on (session basename, name)
    status: completed
  - id: browser-param
    content: Add most_recent_only_log_file_df to show_session_log_browser; overlay + restrict before status/select
    status: completed
  - id: notebook-wire
    content: Pass most_recent_only_log_file_df into show_session_log_browser in greatlakes_file_transfers_notebook
    status: completed
isProject: false
---

# Override log mtimes from Globus DF

## Problem
[`show_session_log_browser`](c:\Users\pho\repos\PhoGlobusHelpers\src\phoglobushelpers\gen_scripts_log_status.py) builds everything from local `Path.stat().st_mtime`. After Globus→Apogee sync, those times are often the copy time, not the true GreatLakes modification time. [`most_recent_only_log_file_df`](c:\Users\pho\repos\PhoGlobusHelpers\notebooks\greatlakes_file_transfers_notebook.ipynb) already has correct Globus `last_modified` (same schema: `parent_path`, `name`, `last_modified`, `size`).

Remote vs local `parent_path` differ, e.g.:
- Globus: `/umms-kdiba/Data/Output/gen_scripts/run_...`
- Local: `K:/scratch/gen_scripts/run_...`

Match key must be `(Path(parent_path).name, name)` (session folder + filename), not full path.

## Approach
In [`gen_scripts_log_status.py`](c:\Users\pho\repos\PhoGlobusHelpers\src\phoglobushelpers\gen_scripts_log_status.py):

1. **Add helper** `apply_remote_mtime_overrides(local_file_df, override_df) -> pd.DataFrame`
   - Build lookup from override: `(session_name, name) -> last_modified` (UTC via `pd.to_datetime(..., utc=True)`).
   - Copy `local_file_df`, set `session` = basename of `parent_path`, map override mtimes onto matching rows.
   - Leave unmatched local rows unchanged.
   - Return the updated frame (local `parent_path` / `size` / paths preserved for reading tails).

2. **When override is provided**, after overlay, **restrict** the working `file_df` to rows that matched an override entry (same session+name). Reason: if FS mtimes are all “recent,” unmatched local historical logs would still win cutoff/sort. The override DF is already the Globus “most recent” set.

3. **Extend** `show_session_log_browser(...)` with:
   ```python
   most_recent_only_log_file_df: Optional[pd.DataFrame] = None,
   ```
   Flow after local scan:
   ```text
   file_df = list_local_gen_scripts_files(root)
   if most_recent_only_log_file_df is not None:
       file_df = apply_remote_mtime_overrides(file_df, most_recent_only_log_file_df)
       # keep only rows that received an override
   status_df = build_session_status_df(file_df, ...)
   recent_logs = select_recent_logs_by_session(file_df, ...)  # uses overridden mtimes + cutoff
   ```
   Docstring: note that when provided, Globus mtimes drive cutoff, session order, accordion headers, and `age_hours`/status; log contents still come from local paths under `root`.

4. **Notebook wire-up** in the browse cell of [`greatlakes_file_transfers_notebook.ipynb`](c:\Users\pho\repos\PhoGlobusHelpers\notebooks\greatlakes_file_transfers_notebook.ipynb):
   ```python
   status_df = show_session_log_browser(
       Path(r"K:/scratch/gen_scripts"),
       most_recent_only_log_file_df=most_recent_only_log_file_df,
   )
   ```
   (Assumes the earlier cell that builds `most_recent_only_log_file_df` has been run.)

## Out of scope
- Overriding `size` (local sizes stay; only mtimes from Globus).
- Accepting `all_log_file_df` (caller passes the already-filtered most-recent frame).
- Changing Globus listing or copy logic.