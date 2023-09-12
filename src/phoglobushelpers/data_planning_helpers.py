from typing import Optional, List, Dict
from pathlib import Path
import psutil # used for `DiskInfo`
import numpy as np
import pandas as pd
from attrs import define, field, Factory, asdict


@define(slots=False)
class AbstractEndpoint:
    """ Represents any data transfer endpoint like a hard-drive filesystem, a virtual filesystem spanning many physical drives (like a RAID array), a cloud or network file resource (like a computer on the LAN, a cloud storage service like Google Drive or Dropbox, etc).
    
    Example:
        from phoglobushelpers.data_planning_helpers import AbstractEndpoint
        MAX_Disk = AbstractEndpoint("MAX", 14.4*1024.0) # 14.4TB disk
        HugePort_Disk = AbstractEndpoint("HugePort", 18.2*1024.0, filesystem='exfat', mount_point=Path('/media/HugePort')) # 17.3TB
        HUUUGE_Disk = AbstractEndpoint("HUUUGE", 18.2*1024.0, filesystem='exfat', mount_point=Path('/run/media/halechr/HUUUGE')) # 18.2TB Disk


    """
    # uuid: int # globally unique identifier
    name: str
    capacity_GB: float # total storage capacity in GigaBytes (GB)
    transfer_rate_GB_per_sec: Optional[float] # average transfer rate in GB/sec
    filesystem: Optional[str]
    mount_point: Optional[Path]
    

@define(slots=False)
class DiskInfo:
    """ Displays info about a partition of a disk.
     
    Uses `psutil` to get information about the available disks from the system

    # Usage
        from phoglobushelpers.data_planning_helpers import get_mounted_disks_info, DiskInfo
        df = get_mounted_disks_info()
        display(df)
        included_mounts = ['/media/MAX', '/run/media/halechr/HUUUGE', '/media/HugePort']
        df = df[np.isin(df['mount_point'], included_mounts)]
        df
    """
    device: str
    mount_point: str
    fstype: str
    total: int
    used: int
    free: int
    percent: float

def get_mounted_disks_info(included_mounts=None):
    partitions = psutil.disk_partitions()
    disks = []

    for partition in partitions:
        # Filter based on included_mounts
        if included_mounts and partition.mountpoint not in included_mounts:
            continue

        usage = psutil.disk_usage(partition.mountpoint)
        disk = DiskInfo(
            device=partition.device,
            mount_point=partition.mountpoint,
            fstype=partition.fstype,
            total=usage.total / (1e9),  # Convert bytes to GB
            used=usage.used / (1e9),    # Convert bytes to GB
            free=usage.free / (1e9),    # Convert bytes to GB
            percent=usage.percent
        )
        disks.append(disk)

    return pd.DataFrame([asdict(disk) for disk in disks])




