from typing import Optional, List, Dict
from attrs import define, field, Factory
from pathlib import Path

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
    






