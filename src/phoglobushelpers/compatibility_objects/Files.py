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
    

    def to_dataframe(self, enable_permissions_columns: bool = False, enable_all_columns: bool = False) -> pd.DataFrame:
        """ 
        Default Columns: ['type', 'parent_path', 'name', 'last_modified', 'size']
        Permissions Columns: ['permissions', 'user', 'group']
        Link Columns: ['link_group', 'link_last_modified', 'link_size', 'link_target', 'link_user'], usually empty
        All Columns: ['group', 'last_modified', 'link_group', 'link_last_modified', 'link_size', 'link_target', 'link_user', 'name', 'permissions', 'size', 'type', 'user', 'parent_path']
        
        """
        data_dict = {
            "type": [file.type.value for file in self.DATA],
            "parent_path": [file.parent_path for file in self.DATA],
            "name": [file.name for file in self.DATA],
            "last_modified": [file.last_modified for file in self.DATA],
            "size": [file.size for file in self.DATA],
        }

        if enable_permissions_columns or enable_all_columns:
            data_dict["permissions"] = [file.permissions for file in self.DATA]
            data_dict["user"] = [file.user for file in self.DATA]
            data_dict["group"] = [file.group for file in self.DATA]

        if enable_all_columns:
            data_dict["link_group"] = [file.link_group for file in self.DATA]
            data_dict["link_last_modified"] = [file.link_last_modified for file in self.DATA]
            data_dict["link_size"] = [file.link_size for file in self.DATA]
            data_dict["link_target"] = [file.link_target for file in self.DATA]
            data_dict["link_user"] = [file.link_user for file in self.DATA]

        df = pd.DataFrame(data_dict)
        return df

    def build_file_paths(self) -> List[str]:
        """Builds a list of file paths by combining the parent_path and name attributes."""
        file_paths = []
        for file in self.DATA:
            # Combine parent_path and name, taking care not to add duplicate slashes
            path = file.parent_path.rstrip('/') + '/' + file.name
            file_paths.append(path)
        return file_paths
    



