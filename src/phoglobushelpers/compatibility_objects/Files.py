from typing import List, Dict, Optional, Any
from attrs import define, field, Factory
from enum import Enum
import pandas as pd


""" 

# Initialize the FileList object
file_list = FileList(
	DATA_TYPE=response_dict['DATA_TYPE'],
	DATA=[File(
		group=item['group'],
        last_modified=item['last_modified'],
        link_group=item['link_group'], link_last_modified=item['link_last_modified'], link_size=item['link_size'], link_target=item['link_target'], link_user=item['link_user'],
		name=item['name'],
		permissions=item['permissions'],
        size=item['size'],
		type=FilesystemDataType(item['type']),
        user=item['user']
	) for item in response_dict['DATA']],
    absolute_path=response_dict['absolute_path'],
    endpoint=response_dict['endpoint'],
    length=response_dict['length'],
    path=response_dict['path'],
    rename_supported=response_dict['rename_supported'],
    symlink_supported=response_dict['symlink_supported'],
    total=response_dict['total'],
)
file_list

"""
class FilesystemDataType(Enum):
	"""Docstring for FilesystemDataType."""
	DIRECTORY = "dir"
	FILE = "file"
	
	
@define(slots=False)
class File:
    """ 
    {
      "DATA_TYPE": "file",
      "group": "umms-kdiba-turbo",
      "last_modified": "2023-04-17 23:40:01+00:00",
      "link_group": null,
      "link_last_modified": null,
      "link_size": null,
      "link_target": null,
      "link_user": null,
      "name": "KDIBA",
      "permissions": "2770",
      "size": 106,
      "type": "dir",
      "user": "halechr"
    },
    """
    # DATA_TYPE: str # ignores DATA_TYPE
    group: str
    last_modified: str
    link_group: Optional[str]
    link_last_modified: Optional[str]
    link_size: Optional[str]
    link_target: Optional[str]
    link_user: Optional[str]
    name: str
    permissions: str
    size: int
    type: FilesystemDataType
    user: str
    parent_path: str

@define(slots=False)
class FileList:
    """ 
    {
		...,
		{
		"DATA_TYPE": "file",
		"group": "umms-kdiba-turbo",
		"last_modified": "2023-05-20 04:37:33+00:00",
		"link_group": null,
		"link_last_modified": null,
		"link_size": null,
		"link_target": null,
		"link_user": null,
		"name": "new_global_batch_result.pkl",
		"permissions": "0770",
		"size": 6656,
		"type": "file",
		"user": "halechr"
		}
	],
	"DATA_TYPE": "file_list",
	"absolute_path": "/umms-kdiba/Data/",
	"endpoint": "8c185a84-5c61-4bbc-b12b-11430e20010f",
	"length": 19,
	"path": "/umms-kdiba/Data/",
	"rename_supported": true,
	"symlink_supported": false,
	"total": 19
	})

    
    """
    DATA: List[File]
    DATA_TYPE: str
    absolute_path: str
    endpoint: str
    length: int
    path: str
    rename_supported: bool
    symlink_supported: bool
    total: int
    

    def to_dataframe(self) -> pd.DataFrame:
        data_dict = {
            "group": [],
            "last_modified": [],
            "link_group": [],
            "link_last_modified": [],
            "link_size": [],
            "link_target": [],
            "link_user": [],
            "name": [],
            "permissions": [],
            "size": [],
            "type": [],
            "user": [],
            "parent_path": []  # New field for parent path
        }

        for file in self.DATA:
            data_dict["parent_path"].append(file.parent_path)  # Add parent path to the data
            data_dict["name"].append(file.name)
            data_dict["last_modified"].append(file.last_modified)
            data_dict["link_group"].append(file.link_group)
            data_dict["link_last_modified"].append(file.link_last_modified)
            data_dict["link_size"].append(file.link_size)
            data_dict["link_target"].append(file.link_target)
            data_dict["link_user"].append(file.link_user)
            data_dict["group"].append(file.group)
            data_dict["permissions"].append(file.permissions)
            data_dict["size"].append(file.size)
            data_dict["type"].append(file.type.value)
            data_dict["user"].append(file.user)
            

        df = pd.DataFrame(data_dict)
        return df





