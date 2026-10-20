---
name: Copy path DataFrame
overview: Expose the source→dest path mapping that `perform_copy_files` already computes as a DataFrame, so notebook calls can show where each file will land on the destination.
todos:
  - id: build-path-df
    content: Add build_copy_path_df(active_file_df, transfer) in PhoGlobusHelper.py
    status: completed
  - id: refactor-perform-copy
    content: Refactor perform_copy_files to use it, add dry_run, return path DataFrame
    status: completed
  - id: update-notebook
    content: Update greatlakes_file_transfers_notebook cell to capture/display transfer_path_df
    status: completed
isProject: false
---

# Return source/dest transfer path DataFrame

## Context

[`perform_copy_files`](c:\Users\pho\repos\PhoGlobusHelpers\src\phoglobushelpers\PhoGlobusHelper.py) already builds dest paths but returns Globus task dicts (and notebook callers never use that return):

```528:531:src/phoglobushelpers/PhoGlobusHelper.py
active_file_paths: List[Path] = [Path(row.parent_path).joinpath(row.name) for row in active_file_df.itertuples(name='LogFile')]
endpoint_relative_src_lines: List[str] = [str(a_line.as_posix()) for a_line in active_file_paths]
endpoint_relative_dest_lines: List[str] = [a_line.replace(transfer.source_endpoint_path, transfer.dest_path_root, 1) for a_line in endpoint_relative_src_lines]
```

## Approach

In [`PhoGlobusHelper.py`](c:\Users\pho\repos\PhoGlobusHelpers\src\phoglobushelpers\PhoGlobusHelper.py):

1. Add `build_copy_path_df(active_file_df, transfer) -> pd.DataFrame` with columns:
   - `source_path`
   - `dest_path`
   - plus original listing columns from `active_file_df` (`name`, `parent_path`, etc.) so the preview stays useful

2. Refactor `perform_copy_files` to:
   - call `build_copy_path_df`
   - add `dry_run: bool = False`
   - if `dry_run=True`: return the path DataFrame only (no Globus transfer)
   - if `dry_run=False`: run `batch_transfer_files` as today, then **return the path DataFrame** (task dicts are currently unused by all notebook callers)

3. Update the highlighted cell in [`greatlakes_file_transfers_notebook.ipynb`](c:\Users\pho\repos\PhoGlobusHelpers\notebooks\greatlakes_file_transfers_notebook.ipynb) to capture and display the result, e.g.:

```python
active_file_df = deepcopy(most_recent_only_log_file_df)
transfer_path_df = connect_man.perform_copy_files(
    active_file_df=active_file_df,
    transfer=gen_scripts_GL_to_Apogee,
    synchronous_wait=False,
)
transfer_path_df
```

No other notebooks need updates for this to work; their calls keep transferring and simply receive a DataFrame instead of unused task dicts.

## Usage after change

- Preview only: `connect_man.perform_copy_files(..., dry_run=True)` or `connect_man.build_copy_path_df(...)`
- Transfer + see destinations: assign the return value of `perform_copy_files` as above
