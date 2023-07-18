from typing import List
from attrs import define, field, Factory
from enum import Enum


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
    DATA_TYPE: str
    group: str
    last_modified: str
    link_group: str
    link_last_modified: str
    link_size: str
    link_target: str
    link_user: str
    name: str
    permissions: str
    size: int
    type: str
    user: str

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
    

