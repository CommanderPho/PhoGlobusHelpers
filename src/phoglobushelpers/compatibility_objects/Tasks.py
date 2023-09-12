from typing import List, Dict, Optional, Any
from attrs import define, field, Factory
from enum import Enum
import pandas as pd

""" 

from phoglobushelpers.compatibility_objects.Tasks import FatalError, Task, TaskList

transfer_client = connect_man.transfer_client
# tasks_list = transfer_client.task_list()
# tasks_list

response_dict = transfer_client.task_list()
# Initialize the TaskList object
tasks_list = TaskList(
    DATA_TYPE=response_dict['DATA_TYPE'],
    DATA=[Task(**item) for item in response_dict['DATA']],
    length=response_dict['length'],
    limit=response_dict['limit'],
    offset=response_dict['offset'],
    total=response_dict['total']
)
tasks_list.to_dataframe()


"""
@define(slots=False)
class FatalError:
    code: str
    description: str
    
	
@define(slots=False)
class Task:
    """ 
    {
      "DATA_TYPE": "task",
      "bytes_checksummed": 1534112401584,
      "bytes_transferred": 0,
      "canceled_by_admin": null,
      "canceled_by_admin_message": null,
      "command": "API 0.10",
      "completion_time": "2023-04-20T21:12:02+00:00",
      "deadline": "2023-04-20T21:11:03+00:00",
      "delete_destination_extra": false,
      "destination_endpoint": "u_gjz3ny5efnehvgt65z3lxdqd74#b82d3b90-7b07-11ed-b303-55098fa75e99",
      "destination_endpoint_display_name": "UMich ARC Non-Sensitive Data Den Volume Collection",
      "destination_endpoint_id": "ab65757f-00f5-4e5b-aa21-133187732a01",
      "directories": 5618,
      "effective_bytes_per_second": 0,
      "encrypt_data": false,
      "fail_on_quota_errors": true,
      "fatal_error": {
        "code": "EXPIRED",
        "description": "deadline expired"
      },
      "faults": 141,
      "files": 54927,
      "files_skipped": 1400,
      "files_transferred": 0,
      "filter_rules": null,
      "history_deleted": true,
      "is_ok": null,
      "is_paused": false,
      "label": "Timer Hiro, Jahangir, KDIBA, Kourosh, Laurel, Rachel, run 1",
      "nice_status": null,
      "nice_status_details": null,
      "nice_status_expires_in": null,
      "nice_status_short_description": null,
      "owner_id": "a3d80fc3-63c9-490c-9b9d-e5941faf1027",
      "preserve_timestamp": false,
      "recursive_symlinks": "ignore",
      "request_time": "2023-04-19T21:09:12+00:00",
      "skip_source_errors": true,
      "source_endpoint": null,
      "source_endpoint_display_name": null,
      "source_endpoint_id": null,
      "status": "FAILED",
      "subtasks_canceled": 0,
      "subtasks_expired": 53527,
      "subtasks_failed": 0,
      "subtasks_pending": 0,
      "subtasks_retrying": 0,
      "subtasks_skipped_errors": 0,
      "subtasks_succeeded": 7024,
      "subtasks_total": 60551,
      "symlinks": 0,
      "sync_level": 3,
      "task_id": "6e5e35fc-def6-11ed-9b9b-c9bb788c490e",
      "type": "TRANSFER",
      "username": "u_upma7q3dzfeqzg454wkb7lyqe4",
      "verify_checksum": true
    }
    
    {'DATA_TYPE': 'task', 'bytes_checksummed': 0, 'bytes_transferred': 246415360, 'canceled_by_admin': None, 'canceled_by_admin_message': None, 'command': 'API 0.10 go', 'completion_time': '2023-06-29T12:32:08+00:00', 'deadline': '2023-07-02T12:10:41+00:00', 'delete_destination_extra': False, 'destination_base_path': None, 'destination_endpoint': 'u_upma7q3dzfeqzg454wkb7lyqe4#84991054-07b4-11ed-8d83-a54cf61939f8', 'destination_endpoint_display_name': 'Apogee', 'destination_endpoint_id': '84991054-07b4-11ed-8d83-a54cf61939f8', 'destination_local_user': None, ...}
    
    ['DATA_TYPE', 'bytes_checksummed', 'bytes_transferred', 'canceled_by_admin', 'canceled_by_admin_message', 'command', 'completion_time', 'deadline', 'delete_destination_extra', 'destination_base_path', 'destination_endpoint', 'destination_endpoint_display_name', 'destination_endpoint_id', 'destination_local_user', 'destination_local_user_status', 'directories', 'effective_bytes_per_second', 'encrypt_data', 'fail_on_quota_errors', 'fatal_error', 'faults', 'files', 'files_skipped', 'files_transferred', 'filter_rules', 'history_deleted', 'is_ok', 'is_paused', 'label', 'nice_status', 'nice_status_details', 'nice_status_expires_in', 'nice_status_short_description', 'owner_id', 'preserve_timestamp', 'recursive_symlinks', 'request_time', 'skip_source_errors', 'source_base_path', 'source_endpoint', 'source_endpoint_display_name', 'source_endpoint_id', 'source_local_user', 'source_local_user_status', 'status', 'subtasks_canceled', 'subtasks_expired', 'subtasks_failed', 'subtasks_pending', 'subtasks_retrying', 'subtasks_skipped_errors', 'subtasks_succeeded', 'subtasks_total', 'symlinks', 'sync_level', 'task_id', 'type', 'username', 'verify_checksum']
    
    """
    DATA_TYPE: str # ignores DATA_TYPE
    bytes_checksummed: int
    bytes_transferred: int
    canceled_by_admin: Optional[str]
    canceled_by_admin_message: Optional[str]
    command: str
    completion_time: str
    deadline: str
    delete_destination_extra: bool
    destination_base_path: Optional[Any] # Added 2023-09-12
    destination_endpoint: Optional[str]
    destination_endpoint_display_name: Optional[str]
    destination_endpoint_id: Optional[str]
    destination_local_user: Optional[Any] # Added 2023-09-12
    destination_local_user_status: Optional[Any] # Added 2023-09-12
    directories: int
    effective_bytes_per_second: int
    encrypt_data: bool
    fail_on_quota_errors: bool
    fatal_error: Optional[FatalError]
    faults: int
    files: int
    files_skipped: int
    files_transferred: int
    filter_rules: Optional[Any]
    history_deleted: bool
    is_ok: Optional[bool]
    is_paused: bool
    label: Optional[str]
    nice_status: Optional[str]
    nice_status_details: Optional[str]
    nice_status_expires_in: Optional[str]
    nice_status_short_description: Optional[str]
    owner_id: str
    preserve_timestamp: bool
    recursive_symlinks: str
    request_time: str
    skip_source_errors: bool
    source_base_path: Optional[Any] # Added 2023-09-12
    source_endpoint: Optional[str]
    source_endpoint_display_name: Optional[str]
    source_endpoint_id: Optional[str]
    source_local_user: Optional[Any] # Added 2023-09-12
    source_local_user_status: Optional[Any] # Added 2023-09-12
    status: str
    subtasks_canceled: int
    subtasks_expired: int
    subtasks_failed: int
    subtasks_pending: int
    subtasks_retrying: int
    subtasks_skipped_errors: int
    subtasks_succeeded: int
    subtasks_total: int
    symlinks: int
    sync_level: Optional[int]
    task_id: str
    type: str
    username: str
    verify_checksum: bool
    
    # missing fields:
    # destination_base_path
    
    def __attrs_post_init__(self):
        if self.fatal_error is not None:
            if isinstance(self.fatal_error, dict):
                self.fatal_error = FatalError(**self.fatal_error) # convert to FatalError type.

@define(slots=False)
class TaskList:
    """
    {
        "DATA": [
            {...},
            {...},
            {...}
        ],
        "DATA_TYPE": "task_list",
        "length": 10,
        "limit": 10,
        "offset": 0,
        "total": 345
    }
    """
    DATA: List[Task]
    DATA_TYPE: str
    length: int
    limit: int
    offset: int
    total: int

    def to_dataframe(self) -> pd.DataFrame:
        data_dict = {
            "bytes_checksummed": [],
            "bytes_transferred": [],
            "canceled_by_admin": [],
            "canceled_by_admin_message": [],
            "command": [],
            "completion_time": [],
            "deadline": [],
            "delete_destination_extra": [],
            "destination_endpoint": [],
            "destination_endpoint_display_name": [],
            "destination_endpoint_id": [],
            "directories": [],
            "effective_bytes_per_second": [],
            "encrypt_data": [],
            "fail_on_quota_errors": [],
            "fatal_error_code": [],
            "fatal_error_description": [],
            "faults": [],
            "files": [],
            "files_skipped": [],
            "files_transferred": [],
            "filter_rules": [],
            "history_deleted": [],
            "is_ok": [],
            "is_paused": [],
            "label": [],
            "nice_status": [],
            "nice_status_details": [],
            "nice_status_expires_in": [],
            "nice_status_short_description": [],
            "owner_id": [],
            "preserve_timestamp": [],
            "recursive_symlinks": [],
            "request_time": [],
            "skip_source_errors": [],
            "source_endpoint": [],
            "source_endpoint_display_name": [],
            "source_endpoint_id": [],
            "status": [],
            "subtasks_canceled": [],
            "subtasks_expired": [],
            "subtasks_failed": [],
            "subtasks_pending": [],
            "subtasks_retrying": [],
            "subtasks_skipped_errors": [],
            "subtasks_succeeded": [],
            "subtasks_total": [],
            "symlinks": [],
            "sync_level": [],
            "task_id": [],
            "type": [],
            "username": [],
            "verify_checksum": []
        }
        
        for task in self.DATA:
            data_dict["bytes_checksummed"].append(task.bytes_checksummed)
            data_dict["bytes_transferred"].append(task.bytes_transferred)
            data_dict["canceled_by_admin"].append(task.canceled_by_admin)
            data_dict["canceled_by_admin_message"].append(task.canceled_by_admin_message)
            data_dict["command"].append(task.command)
            data_dict["completion_time"].append(task.completion_time)
            data_dict["deadline"].append(task.deadline)
            data_dict["delete_destination_extra"].append(task.delete_destination_extra)
            data_dict["destination_endpoint"].append(task.destination_endpoint)
            data_dict["destination_endpoint_display_name"].append(task.destination_endpoint_display_name)
            data_dict["destination_endpoint_id"].append(task.destination_endpoint_id)
            data_dict["directories"].append(task.directories)
            data_dict["effective_bytes_per_second"].append(task.effective_bytes_per_second)
            data_dict["encrypt_data"].append(task.encrypt_data)
            data_dict["fail_on_quota_errors"].append(task.fail_on_quota_errors)
            if task.fatal_error:
                data_dict["fatal_error_code"].append(task.fatal_error.code)
                data_dict["fatal_error_description"].append(task.fatal_error.description)
            else:
                data_dict["fatal_error_code"].append(None)
                data_dict["fatal_error_description"].append(None)
            data_dict["faults"].append(task.faults)
            data_dict["files"].append(task.files)
            data_dict["files_skipped"].append(task.files_skipped)
            data_dict["files_transferred"].append(task.files_transferred)
            data_dict["filter_rules"].append(task.filter_rules)
            data_dict["history_deleted"].append(task.history_deleted)
            data_dict["is_ok"].append(task.is_ok)
            data_dict["is_paused"].append(task.is_paused)
            data_dict["label"].append(task.label)
            data_dict["nice_status"].append(task.nice_status)
            data_dict["nice_status_details"].append(task.nice_status_details)
            data_dict["nice_status_expires_in"].append(task.nice_status_expires_in)
            data_dict["nice_status_short_description"].append(task.nice_status_short_description)
            data_dict["owner_id"].append(task.owner_id)
            data_dict["preserve_timestamp"].append(task.preserve_timestamp)
            data_dict["recursive_symlinks"].append(task.recursive_symlinks)
            data_dict["request_time"].append(task.request_time)
            data_dict["skip_source_errors"].append(task.skip_source_errors)
            data_dict["source_endpoint"].append(task.source_endpoint)
            data_dict["source_endpoint_display_name"].append(task.source_endpoint_display_name)
            data_dict["source_endpoint_id"].append(task.source_endpoint_id)
            data_dict["status"].append(task.status)
            data_dict["subtasks_canceled"].append(task.subtasks_canceled)
            data_dict["subtasks_expired"].append(task.subtasks_expired)
            data_dict["subtasks_failed"].append(task.subtasks_failed)
            data_dict["subtasks_pending"].append(task.subtasks_pending)
            data_dict["subtasks_retrying"].append(task.subtasks_retrying)
            data_dict["subtasks_skipped_errors"].append(task.subtasks_skipped_errors)
            data_dict["subtasks_succeeded"].append(task.subtasks_succeeded)
            data_dict["subtasks_total"].append(task.subtasks_total)
            data_dict["symlinks"].append(task.symlinks)
            data_dict["sync_level"].append(task.sync_level)
            data_dict["task_id"].append(task.task_id)
            data_dict["type"].append(task.type)
            data_dict["username"].append(task.username)
            data_dict["verify_checksum"].append(task.verify_checksum)
        
        df = pd.DataFrame(data_dict)
        return df