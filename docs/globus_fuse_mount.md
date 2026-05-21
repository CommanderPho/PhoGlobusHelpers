# Globus FUSE Mount

The `globus_fuse_mount.py` script allows mounting a remote Globus collection/endpoint as a local directory using FUSE (Filesystem in Userspace). It caches files locally for read/write operations and falls back to Globus Transfer for transferring files if the collection does not support HTTPS direct access.

## Prerequisites

Ensure you have FUSE installed on your system. In python, the mount depends on `fusepy`.
To install dependencies via `uv`:
```bash
uv add fusepy
uv sync --all-extras
```

## Usage

Run the mounting script using the virtual environment's Python interpreter:

```bash
.venv/bin/python ./scripts/globus_fuse_mount.py <endpoint_uuid_or_bookmark> <local_mountpoint> [options]
```

### Parameters & Arguments

- `endpoint`: The UUID of the Globus collection/endpoint or the name of a Globus Bookmark (case-insensitive name match).
- `mountpoint`: The local path where the filesystem will be mounted.
- `--remote-path`: The directory within the remote Globus endpoint to mount (defaults to `/` or the bookmark's remote path).
- `--foreground`: Run the FUSE filesystem loop in the foreground and log activity details to stdout.
- `--cache-dir`: Local cache directory (defaults to `~/.cache/globus_fuse/<endpoint_id>`).
- `--local-endpoint`: Local Globus Connect Personal endpoint ID for transfer fallback if the endpoint does not support HTTPS.
- `--local-path`: Local directory corresponding to the local endpoint path.

### Example

```bash
.venv/bin/python ./scripts/globus_fuse_mount.py ab65757f-00f5-4e5b-aa21-133187732a01 ~/cloud/globus/DD_Data --foreground
```

---

## Authorization & Consent Flow

When running the mount script:
1. **Initial Login**: It verifies or initializes a general Globus authentication login.
2. **Pre-flight Check**: The script performs a pre-flight test (`operation_ls`) on the endpoint before starting the FUSE daemon.
3. **Handling ConsentRequired**: If access to the specific endpoint requires a custom data access consent, the script will catch the `ConsentRequired` exception, print the required authorization URL, and block to let you authorize and paste the Auth Code.
4. **Daemon Launch**: Once authorized, tokens are cached and the FUSE daemon launches successfully.

---

## Troubleshooting & Clean Unmounting

If the FUSE process terminates unexpectedly, the mount point may get into a stuck state, resulting in errors like:
* `ls: reading directory '...': Transport endpoint is not connected`
* `FileExistsError: [Errno 17] File exists: '...'`

### 1. Check Directory Status
Inspect the status of the mountpoint directory:
```bash
file ~/cloud/globus/DD_Data
```
If it output `Transport endpoint is not connected`, you need to unmount it.

### 2. Standard Unmount
Unmount the filesystem cleanly:
```bash
fusermount -u ~/cloud/globus/DD_Data
```

### 3. Lazy Unmount (Stuck Daemon)
If the unmount is busy or stuck, perform a lazy unmount:
```bash
fusermount -uz ~/cloud/globus/DD_Data
```

### 4. Kill Orphaned Processes
Kill any leftover/zombie mount processes:
```bash
pkill -9 -f globus_fuse_mount.py
```
