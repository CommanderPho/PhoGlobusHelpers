---
name: Preserve transfer timestamps
overview: Add a `preserve_timestamp` option to Globus batch copies (Unix→Windows mtime), plus a local post-hoc helper that can also set Windows creation time from the listing’s `last_modified` after files land on `K:`.
todos:
  - id: wire-preserve-timestamp
    content: Add preserve_timestamp param to batch_transfer_files and perform_copy_files; pass into TransferData
    status: completed
  - id: return-path-df
    content: Have perform_copy_files use build_copy_path_df and return path DataFrame after submit
    status: completed
  - id: local-timestamp-helper
    content: Add apply_local_file_timestamps (os.utime + optional win32_setctime) with /K/→K:/ mapping
    status: completed
  - id: notebook-comment
    content: Add commented post-hoc apply example near gen_scripts transfer / log browser cells
    status: completed
isProject: false
---

# Preserve file timestamps on copy

## Problem

[`batch_transfer_files`](c:\Users\pho\repos\PhoGlobusHelpers\src\phoglobushelpers\PhoGlobusHelper.py) builds `TransferData` with `preserve_timestamp` commented out, so Globus leaves dest mtimes as “now”. After GL→Apogee, [`show_session_log_browser`](c:\Users\pho\repos\PhoGlobusHelpers\src\phoglobushelpers\gen_scripts_log_status.py) on `K:/scratch/gen_scripts` sees wrong ages / ordering.

Globus can preserve **modification** time across Unix→Windows. It does **not** set Windows **creation** (birth) time; that needs a local post-hoc step.

## Approach (chosen)

1. **Primary:** wire Globus `preserve_timestamp` through `batch_transfer_files` ← `perform_copy_files` (works for async notebook transfers; no local wait).
2. **Secondary:** add a local applicator that sets mtime (`os.utime`) and, on Windows, creation time (`win32_setctime`) from `active_file_df['last_modified']`, for callers who want birth time or a post-hoc fix after async completion.

```mermaid
flowchart LR
  GL[GreatLakes source] -->|Globus preserve_timestamp| Apogee["Apogee /K/scratch/..."]
  Apogee --> Local["K:/scratch/..."]
  Local -->|optional apply_local_file_timestamps| Birth["Windows creation = source mtime"]
```

## Changes in [`PhoGlobusHelper.py`](c:\Users\pho\repos\PhoGlobusHelpers\src\phoglobushelpers\PhoGlobusHelper.py)

### 1. `batch_transfer_files`

Add `preserve_timestamp: bool = True` and pass it into `TransferData`:

```python
transfer_options = {"preserve_timestamp": preserve_timestamp}
```

Default **True** (matches the already-commented intent and fixes the log-browser workflow). Callers can pass `False` to keep copy-time mtimes.

### 2. `perform_copy_files`

- Add `preserve_timestamp: bool = True` and forward it to `batch_transfer_files`.
- Align with existing [`build_copy_path_df`](c:\Users\pho\repos\PhoGlobusHelpers\src\phoglobushelpers\PhoGlobusHelper.py): build the path DataFrame first, run the transfer (unless a `dry_run` path already exists), return that DataFrame so notebook/`apply_local_*` callers have `dest_path` + `last_modified`.

### 3. New helper: `apply_local_file_timestamps`

Standalone function (same module or [`path_helpers.py`](c:\Users\pho\repos\PhoGlobusHelpers\src\phoglobushelpers\path_helpers.py)):

- Input: DataFrame with `dest_path` (Globus-style `/K/...`) and `last_modified`.
- Map `/K/...` → `K:/...` (drive-letter path) before touching the filesystem.
- Skip missing files (async transfers often incomplete).
- Always: `os.utime(local, (ts, ts))` from parsed UTC `last_modified`.
- On Windows: `win32_setctime.setctime(local, ts)` when importable; otherwise skip creation time with a one-line warning.
- Return counts: applied / missing / errors.

Optional thin wrapper on `GlobusConnector` that calls this on the path DataFrame. **Do not** auto-run it inside async `perform_copy_files` (files are not on disk yet). Document:

```python
transfer_path_df = connect_man.perform_copy_files(..., preserve_timestamp=True, synchronous_wait=False)
# after transfer completes, if Windows creation time is needed:
apply_local_file_timestamps(transfer_path_df)
```

## Notebook

In [`greatlakes_file_transfers_notebook.ipynb`](c:\Users\pho\repos\PhoGlobusHelpers\notebooks\greatlakes_file_transfers_notebook.ipynb), the gen_scripts transfer cell already captures `transfer_path_df`; leave default on (no API change required). Add a short commented example for the post-hoc applicator after the `show_session_log_browser` cell.

## Out of scope

- No change to `gen_scripts_log_status.py` itself (it already trusts local mtime).
- No new hard dependency if `win32_setctime` is missing (optional import).
- Lab/rMBP dests: Globus mtime preservation alone is enough; creation-time helper is Windows-only.