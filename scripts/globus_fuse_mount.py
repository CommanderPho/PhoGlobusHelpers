#!/usr/bin/env python3
"""
Globus FUSE Filesystem Mount
Mounts a remote Globus endpoint as a local directory using FUSE.

Usage:
    python scripts/globus_fuse_mount.py "KDIBA Lab Turbo - collected_outputs" ~/Globus/Turbo --foreground
"""

import os
import sys
import time
import urllib.parse
import requests
import argparse
from datetime import datetime
from errno import ENOENT, EIO, EACCES, EROFS
import subprocess
import signal

import globus_sdk
from globus_sdk import TransferClient, RefreshTokenAuthorizer, NativeAppAuthClient, TransferData
from fair_research_login import NativeClient
from fuse import FUSE, Operations, LoggingMixIn, FuseOSError

from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from phoglobushelpers.PhoGlobusHelper import GlobusConnector

# Register app details (Matches PhoGlobusHelpers client registration)
CLIENT_ID = '769d24e1-d1cc-4198-9ff7-2626485da449'
APP_NAME = 'PhoGlobusHelpers FUSE Mount'
SCOPES = 'openid email profile urn:globus:auth:scope:transfer.api.globus.org:all'


def download_file(url, token, dest_local_path):
    """Downloads a file directly via HTTPS GET using a Globus Transfer Token."""
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, stream=True)
    if response.status_code == 200:
        os.makedirs(os.path.dirname(dest_local_path), exist_ok=True)
        with open(dest_local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=65536):
                f.write(chunk)
        return True
    else:
        print(f"HTTPS download failed: Status {response.status_code}, Detail: {response.text}")
        return False


def upload_file(url, token, local_path):
    """Uploads a file directly via HTTPS PUT using a Globus Transfer Token."""
    headers = {"Authorization": f"Bearer {token}"}
    with open(local_path, 'rb') as f:
        response = requests.put(url, headers=headers, data=f)
    if response.status_code in (200, 201):
        return True
    else:
        print(f"HTTPS upload failed: Status {response.status_code}, Detail: {response.text}")
        return False


class GlobusFUSE(Operations):
    def __init__(self, transfer_client, endpoint_id, root_path, cache_dir,
                 local_endpoint_id=None, local_endpoint_path=None):
        self.transfer_client = transfer_client
        self.endpoint_id = endpoint_id
        self.root_path = root_path
        self.file_cache = os.path.abspath(cache_dir)
        
        # Local Globus Connect Personal details (optional transfer fallback)
        self.local_endpoint_id = local_endpoint_id
        self.local_endpoint_path = local_endpoint_path
        
        # In-memory caches for listings and timestamps
        self.cache = {}             # {path: [file_dicts]}
        self.cache_timestamps = {}  # {path: timestamp}
        self.pending_uploads = {}   # {file_handle: (mount_path, local_cache_path)}
        
        # Fetch endpoint details to verify HTTPS support
        print(f"Checking HTTPS capability for endpoint {endpoint_id}...")
        try:
            endpoint_info = self.transfer_client.get_endpoint(endpoint_id)
            self.https_server = endpoint_info.get('https_server')
            if self.https_server:
                print(f"HTTPS interface discovered: {self.https_server}")
            else:
                print("Warning: This collection does not advertise an HTTPS interface. File reads will require a local Globus Transfer client.")
        except Exception as e:
            print(f"Failed to query endpoint metadata: {e}")
            self.https_server = None

        # Create cache dir
        os.makedirs(self.file_cache, exist_ok=True)
        print(f"FUSE filesystem initialized. Cache directory: {self.file_cache}")

    @property
    def transfer_at(self):
        """Resolves the current active transfer access token, auto-refreshed by SDK."""
        return self.transfer_client.authorizer.access_token

    def get_remote_path(self, path):
        """Converts a mountpoint-relative path to remote endpoint path."""
        normalized = os.path.normpath(os.path.join(self.root_path, path.lstrip('/')))
        if not normalized.startswith('/'):
            normalized = '/' + normalized
        return normalized

    def _load_directory(self, path):
        """Loads and caches remote directory listing, invalidating after 30 seconds."""
        now = time.time()
        cached = self.cache_timestamps.get(path)
        if cached and (now - cached < 30):
            return
            
        remote_path = self.get_remote_path(path)
        print(f"Fetching directory listing for remote path: {remote_path}")
        try:
            response = self.transfer_client.operation_ls(self.endpoint_id, path=remote_path)
            self.cache[path] = response['DATA']
            self.cache_timestamps[path] = now
        except Exception as e:
            print(f"Error listing directory {remote_path}: {e}")
            raise FuseOSError(EIO)

    def getattr(self, path, fh=None):
        if path == '/':
            return {
                'st_mode': 0o040755,
                'st_nlink': 2,
                'st_size': 4096,
                'st_atime': 0,
                'st_mtime': 0,
                'st_ctime': 0
            }
            
        local_cache_path = os.path.join(self.file_cache, path.lstrip('/'))
        # If the file exists locally (cached or currently being written), return local stat
        if os.path.exists(local_cache_path):
            st = os.lstat(local_cache_path)
            return {
                'st_mode': st.st_mode,
                'st_nlink': st.st_nlink,
                'st_size': st.st_size,
                'st_atime': st.st_atime,
                'st_mtime': st.st_mtime,
                'st_ctime': st.st_ctime
            }
            
        # Fall back to remote cached metadata
        parent_path, name = os.path.split(path)
        cached_items = self.cache.get(parent_path)
        if cached_items is None:
            try:
                self._load_directory(parent_path)
                cached_items = self.cache.get(parent_path, [])
            except Exception:
                cached_items = []
                
        for item in cached_items:
            if item['name'] == name:
                is_dir = item['type'] == 'dir'
                mode = 0o040755 if is_dir else 0o100644
                size = item['size'] if item['size'] is not None else 4096
                
                mtime = 0
                if item.get('last_modified'):
                    try:
                        t_str = item['last_modified'].replace('Z', '+00:00')
                        dt = datetime.fromisoformat(t_str)
                        mtime = int(dt.timestamp())
                    except Exception:
                        pass
                        
                return {
                    'st_mode': mode,
                    'st_nlink': 2 if is_dir else 1,
                    'st_size': size,
                    'st_atime': mtime,
                    'st_mtime': mtime,
                    'st_ctime': mtime
                }
                
        raise FuseOSError(ENOENT)

    def readdir(self, path, fh):
        self._load_directory(path)
        items = self.cache.get(path, [])
        return ['.', '..'] + [item['name'] for item in items]

    def get_cached_file(self, path):
        """Ensures the file is cached locally, downloading it if needed."""
        local_cache_path = os.path.join(self.file_cache, path.lstrip('/'))
        
        # Retrieve metadata
        parent_path, name = os.path.split(path)
        cached_files = self.cache.get(parent_path, [])
        remote_file = None
        for f in cached_files:
            if f['name'] == name:
                remote_file = f
                break
                
        if not remote_file:
            try:
                self._load_directory(parent_path)
                cached_files = self.cache.get(parent_path, [])
                for f in cached_files:
                    if f['name'] == name:
                        remote_file = f
                        break
            except Exception:
                pass
                
        if not remote_file or remote_file['type'] != 'file':
            return None
            
        remote_size = remote_file['size']
        remote_mtime = 0
        if remote_file.get('last_modified'):
            try:
                dt = datetime.fromisoformat(remote_file['last_modified'].replace('Z', '+00:00'))
                remote_mtime = int(dt.timestamp())
            except Exception:
                pass
                
        # Check if local cache is valid
        if os.path.exists(local_cache_path):
            local_stat = os.stat(local_cache_path)
            if local_stat.st_size == remote_size and local_stat.st_mtime >= remote_mtime:
                print(f"FUSE Cache Hit: {path}")
                return local_cache_path
                
        # Cache miss - download required
        print(f"FUSE Cache Miss: {path}. Downloading...")
        remote_path = self.get_remote_path(path)
        os.makedirs(os.path.dirname(local_cache_path), exist_ok=True)
        
        success = False
        if self.https_server:
            quoted_path = urllib.parse.quote(remote_path)
            url = f"{self.https_server.rstrip('/')}{quoted_path}"
            try:
                success = download_file(url, self.transfer_at, local_cache_path)
            except Exception as e:
                print(f"HTTPS download failed for {path}: {e}")
                
        if not success and self.local_endpoint_id and self.local_endpoint_path:
            try:
                success = self.download_via_transfer(remote_path, local_cache_path)
            except Exception as e:
                print(f"Globus Transfer fallback failed for {path}: {e}")
                
        if success:
            if remote_mtime > 0:
                os.utime(local_cache_path, (remote_mtime, remote_mtime))
            return local_cache_path
            
        return None

    def download_via_transfer(self, remote_path, local_cache_path):
        """Downloads files via standard Globus Transfer using a local personal endpoint."""
        relative_path = os.path.relpath(local_cache_path, self.file_cache)
        local_endpoint_dest_path = os.path.normpath(os.path.join(self.local_endpoint_path, relative_path))
        
        tdata = TransferData(
            self.transfer_client,
            self.endpoint_id,
            self.local_endpoint_id,
            label=f"FUSE Cache Download {os.path.basename(remote_path)}"
        )
        tdata.add_item(remote_path, local_endpoint_dest_path)
        
        task = self.transfer_client.submit_transfer(tdata)
        task_id = task['task_id']
        print(f"Submitted download transfer task {task_id}. Polling for completion...")
        
        while True:
            status = self.transfer_client.get_task(task_id)
            task_status = status['status']
            if task_status == 'SUCCEEDED':
                return True
            elif task_status == 'FAILED':
                print(f"Transfer task failed: {status.get('fatal_error')}")
                return False
            time.sleep(1.0)

    def upload_via_transfer(self, local_cache_path, remote_path):
        """Uploads files via standard Globus Transfer using a local personal endpoint."""
        relative_path = os.path.relpath(local_cache_path, self.file_cache)
        local_endpoint_src_path = os.path.normpath(os.path.join(self.local_endpoint_path, relative_path))
        
        tdata = TransferData(
            self.transfer_client,
            self.local_endpoint_id,
            self.endpoint_id,
            label=f"FUSE Cache Upload {os.path.basename(remote_path)}"
        )
        tdata.add_item(local_endpoint_src_path, remote_path)
        
        task = self.transfer_client.submit_transfer(tdata)
        task_id = task['task_id']
        print(f"Submitted upload transfer task {task_id}. Polling for completion...")
        
        while True:
            status = self.transfer_client.get_task(task_id)
            task_status = status['status']
            if task_status == 'SUCCEEDED':
                return True
            elif task_status == 'FAILED':
                print(f"Transfer task failed: {status.get('fatal_error')}")
                return False
            time.sleep(1.0)

    def open(self, path, flags):
        local_path = self.get_cached_file(path)
        if not local_path:
            raise FuseOSError(EIO)
        
        fh = os.open(local_path, flags)
        
        # If open contains write flags, track for upload
        is_write = (flags & (os.O_WRONLY | os.O_RDWR)) != 0
        if is_write:
            self.pending_uploads[fh] = (path, local_path)
            
        return fh

    def create(self, path, mode, fi=None):
        local_path = os.path.join(self.file_cache, path.lstrip('/'))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        fh = os.open(local_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
        
        parent_path, name = os.path.split(path)
        if parent_path not in self.cache:
            self.cache[parent_path] = []
        self.cache[parent_path].append({
            'name': name,
            'type': 'file',
            'size': 0,
            'last_modified': datetime.utcnow().isoformat() + 'Z'
        })
        
        self.pending_uploads[fh] = (path, local_path)
        return fh

    def read(self, path, size, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, size)

    def write(self, path, data, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, data)

    def truncate(self, path, length, fh=None):
        local_path = os.path.join(self.file_cache, path.lstrip('/'))
        if os.path.exists(local_path):
            with open(local_path, 'r+b') as f:
                f.truncate(length)

    def release(self, path, fh):
        os.close(fh)
        
        # Upload if modified/written
        if fh in self.pending_uploads:
            path, local_path = self.pending_uploads.pop(fh)
            print(f"File {path} closed after modification. Syncing back to Globus...")
            remote_path = self.get_remote_path(path)
            
            success = False
            if self.https_server:
                quoted_path = urllib.parse.quote(remote_path)
                url = f"{self.https_server.rstrip('/')}{quoted_path}"
                try:
                    success = upload_file(url, self.transfer_at, local_path)
                except Exception as e:
                    print(f"HTTPS upload failed: {e}")
                    
            if not success and self.local_endpoint_id and self.local_endpoint_path:
                try:
                    success = self.upload_via_transfer(local_path, remote_path)
                except Exception as e:
                    print(f"Globus Transfer fallback upload failed: {e}")
                    
            if success:
                print(f"Successfully synced {path} to Globus.")
                # Update cached listing size
                parent_path, name = os.path.split(path)
                cached_files = self.cache.get(parent_path, [])
                for f in cached_files:
                    if f['name'] == name:
                        f['size'] = os.path.getsize(local_path)
                        f['last_modified'] = datetime.utcnow().isoformat() + 'Z'
                        break
            else:
                print(f"Error: Failed to sync {path} to Globus!")
                
        return 0

    def mkdir(self, path, mode):
        remote_path = self.get_remote_path(path)
        print(f"Creating remote directory: {remote_path}")
        try:
            self.transfer_client.operation_mkdir(self.endpoint_id, remote_path)
            # Invalidate parent listing cache
            parent_path, _ = os.path.split(path)
            self.cache.pop(parent_path, None)
            self.cache_timestamps.pop(parent_path, None)
        except Exception as e:
            print(f"Failed to create directory {remote_path}: {e}")
            raise FuseOSError(EIO)

    def rename(self, old, new):
        old_remote = self.get_remote_path(old)
        new_remote = self.get_remote_path(new)
        print(f"Renaming remote path: {old_remote} -> {new_remote}")
        try:
            self.transfer_client.operation_rename(self.endpoint_id, old_remote, new_remote)
            # Invalidate parent listing caches
            old_parent, _ = os.path.split(old)
            new_parent, _ = os.path.split(new)
            for p in (old_parent, new_parent):
                self.cache.pop(p, None)
                self.cache_timestamps.pop(p, None)
        except Exception as e:
            print(f"Failed to rename {old_remote} to {new_remote}: {e}")
            raise FuseOSError(EIO)

    def unlink(self, path):
        remote_path = self.get_remote_path(path)
        print(f"Deleting remote file: {remote_path}")
        try:
            ddata = globus_sdk.DeleteData(self.transfer_client, self.endpoint_id, recursive=False)
            ddata.add_item(remote_path)
            task = self.transfer_client.submit_delete(ddata)
            task_id = task['task_id']
            print(f"Submitted delete task {task_id}. Polling for completion...")
            
            while True:
                status = self.transfer_client.get_task(task_id)
                if status['status'] == 'SUCCEEDED':
                    break
                elif status['status'] == 'FAILED':
                    raise Exception("Delete task failed")
                time.sleep(0.5)
                
            # Invalidate cache
            parent_path, name = os.path.split(path)
            if parent_path in self.cache:
                self.cache[parent_path] = [f for f in self.cache[parent_path] if f['name'] != name]
                
            # Remove from local cache
            local_path = os.path.join(self.file_cache, path.lstrip('/'))
            if os.path.exists(local_path):
                os.remove(local_path)
        except Exception as e:
            print(f"Failed to delete {path}: {e}")
            raise FuseOSError(EIO)

    def rmdir(self, path):
        # In Globus, deleting directories is handled identically to files via submit_delete
        self.unlink(path)


def main():
    parser = argparse.ArgumentParser(description="Mount a Globus Endpoint as a local FUSE filesystem.")
    parser.add_argument("endpoint", help="Globus endpoint UUID or Bookmark name")
    parser.add_argument("mountpoint", help="Local directory to mount the filesystem")
    parser.add_argument("--remote-path", default=None, help="Remote path to mount (defaults to bookmark path or '/')")
    parser.add_argument("--local-endpoint", default=None, help="Local Globus Connect Personal endpoint ID for transfer fallback")
    parser.add_argument("--local-path", default=None, help="Local directory corresponding to the local endpoint")
    parser.add_argument("--cache-dir", default=None, help="Local cache directory (defaults to ~/.cache/globus_fuse/<endpoint_id>)")
    parser.add_argument("--foreground", action="store_true", help="Run FUSE in the foreground and log info")
    args = parser.parse_args()

    mountpoint = os.path.abspath(args.mountpoint)
    if not os.path.exists(mountpoint):
        os.makedirs(mountpoint, exist_ok=True)

    # 1. Login and setup TransferClient (using fair-research-login)
    print("Initializing Globus login...")
    client = NativeClient(client_id=CLIENT_ID, app_name=APP_NAME)
    try:
        tokens = client.load_tokens(requested_scopes=SCOPES)
    except Exception:
        tokens = None

    if not tokens:
        print("No cached tokens found. Launching browser authentication...")
        tokens = client.login(requested_scopes=SCOPES, refresh_tokens=True)
        try:
            client.save_tokens(tokens)
            print("Tokens cached successfully.")
        except Exception as e:
            print(f"Failed to save tokens: {e}")

    transfer_tokens = tokens['transfer.api.globus.org']
    authorizer = RefreshTokenAuthorizer(
        transfer_tokens['refresh_token'],
        NativeAppAuthClient(client_id=CLIENT_ID),
        access_token=transfer_tokens['access_token'],
        expires_at=transfer_tokens['expires_at_seconds']
    )
    transfer_client = TransferClient(authorizer=authorizer)
    
    # Instantiate connector to retrieve bookmarks
    connector = GlobusConnector(transfer_client=transfer_client)

    # 2. Resolve endpoint name / Bookmark
    endpoint_id = args.endpoint
    remote_path = args.remote_path
    
    print("Checking bookmarks for name matches...")
    try:
        bookmarks = connector.get_bookmarks().DATA
        for b in bookmarks:
            if b.name.strip().lower() == endpoint_id.strip().lower():
                print(f"Found matching bookmark '{b.name}': {b.endpoint_id} ({b.path})")
                endpoint_id = b.endpoint_id
                if remote_path is None:
                    remote_path = b.path
                break
    except Exception as e:
        print(f"Could not load bookmarks ({e}), treating endpoint as raw UUID.")

    if remote_path is None:
        remote_path = "/"

    # Pre-flight check to handle ConsentRequired errors before mounting
    print("Performing pre-flight check on remote endpoint...")
    try:
        transfer_client.operation_ls(endpoint_id, path=remote_path)
    except globus_sdk.TransferAPIError as err:
        if err.info.consent_required:
            print("Encountered a ConsentRequired error. You must login to grant consents.")
            scopes = err.info.consent_required.required_scopes
            print(f"Requesting additional scopes: {scopes}")
            tokens = client.login(requested_scopes=scopes, refresh_tokens=True, force=True)
            try:
                client.save_tokens(tokens)
                print("Tokens cached successfully.")
            except Exception as e:
                print(f"Failed to save tokens: {e}")
            
            # Rebuild authorizer and transfer_client
            transfer_tokens = tokens['transfer.api.globus.org']
            authorizer = RefreshTokenAuthorizer(
                transfer_tokens['refresh_token'],
                NativeAppAuthClient(client_id=CLIENT_ID),
                access_token=transfer_tokens['access_token'],
                expires_at=transfer_tokens['expires_at_seconds']
            )
            transfer_client = TransferClient(authorizer=authorizer)
        else:
            print(f"Pre-flight API check failed: {err}")
            sys.exit(1)
    except Exception as e:
        print(f"Pre-flight check failed: {e}")
        sys.exit(1)

    # 3. Setup caching dir
    cache_dir = args.cache_dir
    if cache_dir is None:
        cache_dir = os.path.expanduser(f"~/.cache/globus_fuse/{endpoint_id}")

    # Set up signal handler for clean unmount on Ctrl+C
    def cleanup_unmount(sig, frame):
        print(f"\nUnmounting {mountpoint}...")
        subprocess.run(["fusermount", "-u", mountpoint])
        sys.exit(0)

    signal.signal(signal.SIGINT, cleanup_unmount)
    signal.signal(signal.SIGTERM, cleanup_unmount)

    # 4. Mount
    print(f"Mounting {endpoint_id}:{remote_path} to {mountpoint}...")
    operations = GlobusFUSE(
        transfer_client=transfer_client,
        endpoint_id=endpoint_id,
        root_path=remote_path,
        cache_dir=cache_dir,
        local_endpoint_id=args.local_endpoint,
        local_endpoint_path=args.local_path
    )
    
    # Start FUSE loop
    FUSE(operations, mountpoint, foreground=args.foreground, nothreads=True)


if __name__ == "__main__":
    main()
