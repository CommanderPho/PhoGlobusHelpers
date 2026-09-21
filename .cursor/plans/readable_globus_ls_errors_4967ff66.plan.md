---
name: Readable Globus LS Errors
overview: Make Globus directory listing failures readable, and in the multi-session builder skip missing per-session paths with a warning instead of aborting the whole run.
todos:
  - id: list-files-wrap
    content: In list_files, re-raise non-consent TransferAPIError as FileNotFoundError/RuntimeError with err.message and from None
    status: completed
  - id: build-skip-missing
    content: In build_all_sessions loop, catch FileNotFoundError, warn, skip session; handle empty concat
    status: completed
  - id: day-ls-wrap
    content: Wrap day-folder operation_ls with the same readable error message on failure
    status: completed
isProject: false
---

# Readable Globus LS Errors + Skip Missing Sessions

## Decision
When a per-session subpath is missing (404), **skip that session**, print a short warning using Globus’s `err.message`, and continue aggregating the others. Other listing failures still raise, but with a short human-readable message instead of the raw SDK dump.

## Where to change
Primary file: [`src/phoglobushelpers/PhoGlobusHelper.py`](c:\Users\pho\repos\PhoGlobusHelpers\src\phoglobushelpers\PhoGlobusHelper.py)

### 1. Cleaner re-raise in `list_files`
In the existing `except globus_sdk.TransferAPIError` block (~298–300), keep ConsentRequired handling as-is. For all other TransferAPIErrors, re-raise a plain `FileNotFoundError` (for `ClientError.NotFound`) or `RuntimeError` (other codes) whose message is `err.message` (the readable string like `Directory '...' not found on endpoint '...'`), using `raise ... from None` so Jupyter does not print the long SDK chain.

Pattern already used in-repo: `tapie.message` in [`scripts/automation-examples/globus_folder_sync.py`](c:\Users\pho\repos\PhoGlobusHelpers\scripts\automation-examples\globus_folder_sync.py).

### 2. Skip missing sessions in `build_all_sessions_temp_individual_posterior_files_dir`
In the per-session loop (~507–513), wrap `get_greatlakes_temp_individual_posteriors_files(...)` in `try/except FileNotFoundError` (and optionally `RuntimeError` only if we decide to treat non-404 as hard fail — **hard fail on non-404**). On `FileNotFoundError`:

- `print` a one-line warning including session name + `err` message
- continue to the next session

After the loop, if `all_file_df` is empty, return an empty DataFrame (with expected columns if easy; otherwise empty `pd.DataFrame()`) plus the paths dict, instead of calling `pd.concat([])` which errors.

### 3. Day-level listing
The direct `operation_ls` for the day folder (~474) should use the same readable-error wrapping (small local try/except, or route through `list_files` if straightforward). Missing day folder remains a hard failure with a clear message — that is not a “skip session” case.

## Out of scope
- Notebook cell changes
- Skipping non-404 API errors
- New exception types / public API surface beyond clearer raises and skip-on-missing behavior
