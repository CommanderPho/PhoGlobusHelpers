---
name: log files n-recent
overview: Extend `get_only_most_recent_log_files` to include `*.out` and accept `n_most_recent_files` so callers can keep the top N of each log type per session directory.
todos:
  - id: update-fn
    content: "Update get_only_most_recent_log_files: add .out, n_most_recent_files param, empty-safe groupby/head loop"
    status: completed
  - id: verify
    content: Sanity-check with synthetic DataFrame for n=1 and n=2 across suffixes
    status: completed
isProject: false
---

# Extend most-recent log file selection

**Goal:** Update [`Files.py`](c:\Users\pho\repos\PhoGlobusHelpers\src\phoglobushelpers\compatibility_objects\Files.py) so `get_only_most_recent_log_files` keeps `.err`, `.log`, and `.out`, and returns the `n_most_recent_files` newest of each type per `parent_path`.

**Context:** [`PhoGlobusHelper.get_greatlakes_gen_scripts_log_files`](c:\Users\pho\repos\PhoGlobusHelpers\src\phoglobushelpers\PhoGlobusHelper.py) already lists with `filter="name:~*.log,~*.err,~*.out"`, but the helper currently drops `.out`. Default `n_most_recent_files=1` keeps existing call sites unchanged.

## Behavior

- For each suffix in `('.err', '.log', '.out')`, independently: filter rows, group by `parent_path`, keep the `n` newest by `last_modified`.
- Concatenate and sort by `['parent_path', 'last_modified']` ascending `[True, False]` (same as today).
- Skip empty suffix subsets (avoids current `idxmax` crash when a type is missing).
- If nothing matches any suffix, return an empty DataFrame with the same columns.

## Implementation

Replace the duplicated err/log `idxmax` blocks with a small loop:

```python
def get_only_most_recent_log_files(
    log_file_df: pd.DataFrame, n_most_recent_files: int = 1
) -> pd.DataFrame:
    """Return the n most recent '.err', '.log', and '.out' files per parent_path."""
    df = deepcopy(log_file_df)
    df['last_modified'] = pd.to_datetime(df['last_modified'])

    parts = []
    for suffix in ('.err', '.log', '.out'):
        subset = df[df['name'].str.endswith(suffix)]
        if subset.empty:
            continue
        top_n = (
            subset.sort_values('last_modified', ascending=False)
            .groupby('parent_path', group_keys=False)
            .head(n_most_recent_files)
        )
        parts.append(top_n)

    if not parts:
        return df.iloc[0:0].copy()

    return pd.concat(parts).sort_values(
        by=['parent_path', 'last_modified'], ascending=[True, False]
    )
```

Update the docstring to mention `.out` and `n_most_recent_files`.

## Out of scope

- No changes to [`PhoGlobusHelper.py`](c:\Users\pho\repos\PhoGlobusHelpers\src\phoglobushelpers\PhoGlobusHelper.py) or notebooks (default `n=1` is enough; callers can pass `n_most_recent_files` when needed).
- No new test file unless you ask for one later.

## Verify

- Quick sanity check in a notebook or REPL: call with a small synthetic DataFrame covering multiple sessions, all three suffixes, and `n_most_recent_files=2`; confirm counts and ordering.