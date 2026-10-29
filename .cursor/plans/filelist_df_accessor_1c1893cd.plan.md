---
name: Filelist DF accessor
overview: Add a pandas DataFrame `.filelist` accessor that filters FileList dataframes on the `name` column using glob patterns, then use it in the collected-figures notebook cell.
todos:
  - id: add-accessor
    content: Add FileListDataFrameAccessor with glob name_matches filter in Files.py and register .filelist
    status: completed
  - id: notebook-cell
    content: Use .filelist.filter(name_matches=...) in the collected-figures notebook scratch cell
    status: completed
isProject: false
---

# FileList DataFrame `.filelist.filter` accessor

Add a pandas accessor so FileList tables can be filtered with glob patterns on the existing `name` column:

```python
all_figures_file_df.filelist.filter(name_matches=['*_BatchPhoJonathanReplayFRC_*.png'])
```

Those frames already come from [`FileList.to_dataframe()`](src/phoglobushelpers/compatibility_objects/Files.py) with columns `type`, `parent_path`, `name`, `last_modified`, `size`.

## Implementation

In [`src/phoglobushelpers/compatibility_objects/Files.py`](src/phoglobushelpers/compatibility_objects/Files.py):

- Register `@pd.api.extensions.register_dataframe_accessor("filelist")` at import time (the notebook already imports `File` / `FileList` from this module).
- Guard re-registration so `%autoreload 3` does not warn on every cell rerun.
- `filter(*, name_matches)`:
  - Accept a string or list of strings (normalize a single string to a one-item list).
  - Treat each item as a glob (`fnmatch.fnmatchcase`), not regex, so `*` matches the example pattern.
  - Keep a row if **any** pattern matches `name` (OR).
  - Match the basename in `name` only, not `parent_path`.
  - Use `fnmatchcase` so matching stays case-sensitive on Windows (Globus names are Linux-style).
  - Return a filtered copy of the original DataFrame.
  - Raise `AttributeError` if `name` is missing (pandas accessor convention).

Sketch:

```python
@pd.api.extensions.register_dataframe_accessor("filelist")
class FileListDataFrameAccessor:
    def filter(self, *, name_matches: Union[str, List[str]]) -> pd.DataFrame:
        patterns = [name_matches] if isinstance(name_matches, str) else list(name_matches)
        names = self._obj["name"].astype(str)
        mask = False
        for pattern in patterns:
            mask = mask | names.map(lambda n, p=pattern: fnmatch.fnmatchcase(n, p))
        return self._obj.loc[mask].copy()
```

## Notebook

Replace the scratch strings in [`notebooks/greatlakes_file_transfers_notebook.ipynb`](notebooks/greatlakes_file_transfers_notebook.ipynb) (the cell currently holding `'*_BatchPhoJonathanReplayFRC_*.png'`) with:

```python
all_figures_file_df.filelist.filter(name_matches=['*_BatchPhoJonathanReplayFRC_*.png'])
```

No extra import is required beyond the existing `FileList` import in the first cell.
