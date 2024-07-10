import time
from copy import deepcopy
from collections import deque
from datetime import datetime, date, timedelta
from typing import Any, List, Optional, Dict, Union
from globus_sdk.response import GlobusHTTPResponse
import pandas as pd
import globus_sdk
from globus_sdk import AccessTokenAuthorizer, TransferClient, TransferData, TransferAPIError
from globus_sdk.scopes import TransferScopes
from attrs import define, field, Factory

from phoglobushelpers.compatibility_objects.Bookmarks import Bookmark, BookmarkList
from phoglobushelpers.compatibility_objects.Files import File, FilesystemDataType, FileList, get_only_most_recent_log_files
from phoglobushelpers.compatibility_objects.Tasks import FatalError, Task, TaskList
TransferFilterDict = Dict[str, Union[str, List[str]]]


class KnownEndpoints:
    globus_endpoint_gl_homedir:str='e0370902-9f48-11e9-821b-02b7a92d8e58'
    globus_endpoint_kdiba_lab_turbo:str='8c185a84-5c61-4bbc-b12b-11430e20010f'
    endpoint_LNX00052_Fedora:str = 'af3fcfce-f664-11ed-9a7d-83ef71fbf0ae'
    endpoint_Apogee:str = '84991054-07b4-11ed-8d83-a54cf61939f8' # Pho's personal computer
    

@define(slots=False, repr=False)
class GlobusConnector:
    """ A wrapper that holds an active connection to Globus and allows object-oriented operations to fetch endpoints, bookmarks, files, and perform operations on them.

    See:
        https://globus-sdk-python.readthedocs.io/en/stable/examples/minimal_transfer_script/index.html#example-minimal-transfer

    Usage:
    
        from phoglobushelpers.PhoGlobusHelper import GlobusConnector, KnownEndpoints
        connect_man = GlobusConnector.login_and_get_transfer_client()
        transfer_client = connect_man.transfer_client
        connect_man.list_endpoints()

    """

    # Define the Globus transfer client and authenticate with a user's tokens
    auth_client = globus_sdk.NativeAppAuthClient('769d24e1-d1cc-4198-9ff7-2626485da449')

    # authorizer = field(init=False, default=None)
    transfer_client: TransferClient = field() #, default=None) # TransferClient


    @classmethod
    def login_and_get_transfer_client(cls, *, scopes='openid profile email urn:globus:auth:scope:transfer.api.globus.org:all', copy_to_clipboard=True):
        """ 
            'openid profile email urn:globus:auth:scope:transfer.api.globus.org:all'
        """
        cls.auth_client.oauth2_start_flow(refresh_tokens=True, requested_scopes=scopes)

        authorize_url = cls.auth_client.oauth2_get_authorize_url()
        print(f'Please go to this URL and login: {authorize_url}')
        if copy_to_clipboard:
            df = pd.DataFrame([authorize_url])
            df.to_clipboard(index=False, header=False, sep=',')
            print(f'\t Copied url to clipboard!')

        auth_code = input('Please enter the code you get after login here: ').strip()
        token_response = cls.auth_client.oauth2_exchange_code_for_tokens(auth_code)

        # globus_auth_data = token_response.by_resource_server["auth.globus.org"]
        # get credentials for the Globus Transfer service
        globus_transfer_data = token_response.by_resource_server["transfer.api.globus.org"]
        # the refresh token and access token are often abbreviated as RT and AT
        transfer_rt = globus_transfer_data["refresh_token"]
        transfer_at = globus_transfer_data["access_token"]
        expires_at_s = globus_transfer_data["expires_at_seconds"]

        # construct a RefreshTokenAuthorizer
        # note that `client` is passed to it, to allow it to do the refreshes
        authorizer = globus_sdk.RefreshTokenAuthorizer(transfer_rt, cls.auth_client, access_token=transfer_at, expires_at=expires_at_s)

        # and try using `tc` to make TransferClient calls. Everything should just
        # work -- for days and days, months and months, even years
        transfer_client = globus_sdk.TransferClient(authorizer=authorizer)
        
        return cls(transfer_client=transfer_client)
        

    def list_endpoints(self):
        # high level interface; provides iterators for list responses
        print("My Endpoints:")
        for ep in self.transfer_client.endpoint_search(filter_scope="my-endpoints"):
            print("[{}] {}".format(ep["id"], ep["display_name"]))


    def get_bookmarks(self) -> BookmarkList: # List[Bookmark]
        """ returns a BookmarkList object, containing """
        transfer_client: TransferClient = self.transfer_client
        response_dict = transfer_client.bookmark_list()
        # Initialize the BookmarkList object
        bookmark_list = BookmarkList(
            DATA_TYPE=response_dict['DATA_TYPE'],
            DATA=[Bookmark(
                bookmark_id=item['id'],
                name=item['name'],
                endpoint_id=item['endpoint_id'],
                path=item['path']
            ) for item in response_dict['DATA']]
        )
        # bookmark_list: List[Bookmark] = [Bookmark(bookmark_id=item['id'], name=item['name'], endpoint_id=item['endpoint_id'], path=item['path'] ) for item in response_dict['DATA']]
        return bookmark_list


    def get_tasks(self) -> TaskList:
        """
        tasks_list = connect_man.get_tasks()
        tasks_list.to_dataframe()

        """
        transfer_client: TransferClient = self.transfer_client
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
        return tasks_list


    def batch_transfer_files(self, source_endpoint:str, destination_endpoint:str, filelist_source:List[str], filelist_dest:List[str], label: str = "Batch Transfer", batch_size: int = 100, synchronous_wait:bool=False, max_single_file_wait_time_seconds=60):
        """ performs a batch transfer for the files specified in the filelists from source to endpoint.
        # Set your source and destination endpoint IDs
        # source_endpoint = "SOURCE_ENDPOINT_ID"
        # destination_endpoint = "DESTINATION_ENDPOINT_ID"
        # # Define the list of source and destination files
        # filelist_source = [
        #     "/path/to/source/file1.txt",
        #     "/path/to/source/file2.txt",
        #     "/path/to/source/file3.txt"
        # ]

        # filelist_dest = [
        #     "/path/to/destination/file1.txt",
        #     "/path/to/destination/file2.txt",
        #     "/path/to/destination/file3.txt"
        # ]


        Usage:

        ## INPUTS: src_lines, dest_lines, apogee_gen_scripts_folder_bookmark, lab_Greatlakes_gen_scripts
        endpoint_relative_src_lines = [a_line for a_line in src_lines]
        endpoint_relative_dest_lines = [a_line for a_line in dest_lines]
        
        # Async:
        _transfer_task = connect_man.batch_transfer_files(source_endpoint=lab_Greatlakes_gen_scripts.endpoint_id, destination_endpoint=apogee_gen_scripts_folder_bookmark.endpoint_id, filelist_source=endpoint_relative_src_lines, filelist_dest=endpoint_relative_dest_lines)
        _transfer_task

        # Waiting for completion:
        pending_tasks, (completed_tasks, failed_tasks) = connect_man.batch_transfer_files(source_endpoint=lab_Greatlakes_gen_scripts.endpoint_id, destination_endpoint=apogee_gen_scripts_folder_bookmark.endpoint_id, filelist_source=endpoint_relative_src_lines, filelist_dest=endpoint_relative_dest_lines, synchronous_wait=True)
        completed_tasks

        """
        pending_tasks = {}
        transfer_client: TransferClient = self.transfer_client

        # Set your transfer options
        transfer_options = {
            # "preserve_timestamp": True
        }

        # Turn them all into strings
        filelist_source = [str(path) for path in filelist_source]
        filelist_dest = [str(path) for path in filelist_dest]

        # Verify that the source and destination lists have the same length
        assert len(filelist_source) == len(filelist_dest), "Error: Source and destination file lists must have the same length."

        try:
            total_files: int = len(filelist_source)
            
            # Loop through the file lists and initiate the transfers
            for i in range(0, total_files, batch_size):
                batch_sources = filelist_source[i:i + batch_size]
                batch_dests = filelist_dest[i:i + batch_size]
                
                # Create a TransferData object
                transfer_data = TransferData(
                    transfer_client,
                    source_endpoint,
                    destination_endpoint,
                    label=f"{label} - Batch {i // batch_size + 1}",
                    sync_level="checksum",
                    **transfer_options
                )
                # Add the file transfers to the TransferData object            
                for src_path, dest_path in zip(batch_sources, batch_dests):
                    transfer_data.add_item(src_path, dest_path)
                
                # Initiate the transfer task
                task = transfer_client.submit_transfer(transfer_data)
                print(f"Submitted transfer task {task['task_id']} with {len(batch_sources)} files.")
                pending_tasks[str(task['task_id'])] = task
                
        except TransferAPIError as api_error:
            print(f"Globus API Error: {api_error}")
        except Exception as e:
            print(f"An unexpected error occurred during batch transfer: {e}")


        if synchronous_wait:
            completed_tasks = {}
            failed_tasks = {}
            print(f'because `synchronous_wait == True`, synchronously waiting for {len(pending_tasks)} tasks to complete:')
            for task_id, a_task in pending_tasks.items():
                print(f"Waiting for task {task_id} to complete...")
                did_complete: bool = transfer_client.task_wait(task_id, timeout=max_single_file_wait_time_seconds)
                if did_complete:
                    print(f"Task {task_id} completed.")
                    completed_tasks[task_id] = pending_tasks[task_id] # pending_tasks.pop(task_id)
                else:
                    print(f'Task {task_id} failed or did not complete within max_single_file_wait_time_seconds: {max_single_file_wait_time_seconds}')
                    failed_tasks[task_id] = pending_tasks[task_id] # pending_tasks.pop(task_id)
                    
            print("All transfers completed successfully.")
            return pending_tasks, (completed_tasks, failed_tasks)
        else:
            return pending_tasks
        


    def list_files(self, endpoint: str, path: str, start_date: Optional[str] = None, end_date: Optional[str] = None, should_list_recursively: bool = False, max_depth: int = 1, _current_depth: int = 0, filter: Optional[(str | TransferFilterDict | list[str | TransferFilterDict])] = None) -> FileList:
        """ NOTE: only returns FILES, not FOLDERS/DIRECTORIES
        
        For filter format, see: https://docs.globus.org/api/transfer/file_operations/#dir_listing_filtering
        "filter=type:dir&filter=name:~*.txt" or `filter=["type:=file","type:dir"],`: would match both directories and txt files
        
        """
        if should_list_recursively:
            # make sure that subdirectories are included in the filter
            was_filter_modified: bool = False
            if filter is not None:
                if isinstance(filter, str):
                    filter = [filter,"type:dir"]
                    was_filter_modified = True
                else:
                    # assumed to be a list, tuple, etc
                    if not ("type:dir" in filter):
                        # add it
                        filter.append("type:dir")
                        was_filter_modified = True
                        
            if was_filter_modified:
                print(f'WARNING: filter was modified to include "type:dir" because `should_list_recursively=True`. Otherwise no subdirectories would be returned.')

        transfer_client: TransferClient = self.transfer_client

        try:
            response_dict = transfer_client.operation_ls(
                endpoint_id=endpoint,
                path=path,
                orderby=["last_modified DESC", "name"],
                # orderby=["last_modified", "name"],
                # orderby=["type", "name"],
                filter=filter
            )
        except globus_sdk.TransferAPIError as err:
            if not err.info.consent_required:
                raise

            print("Encountered a ConsentRequired error.\nYou must login a second time to grant consents.\n\n")
            print(f'required scopes:\n{err.info.consent_required.required_scopes}\n')
            transfer_client = self.login_and_get_transfer_client(scopes=err.info.consent_required.required_scopes)

        except Exception as e:
            raise e

        data_files = []
        for item in response_dict['DATA']:
            file_item = File(
                group=item['group'],
                last_modified=item['last_modified'],
                link_group=item['link_group'], link_last_modified=item['link_last_modified'], link_size=item['link_size'], link_target=item['link_target'], link_user=item['link_user'],
                name=item['name'],
                permissions=item['permissions'],
                size=item['size'],
                type=FilesystemDataType(item['type']),
                user=item['user'],
                parent_path=path
            )

            if file_item.type == FilesystemDataType('dir') and should_list_recursively and _current_depth < max_depth:
                # subdirectory_path = f"{path.rstrip('/')}/{file_item.name}"
                subdirectory_path = f"{path}/{file_item.name}" if path != '/' else f"/{file_item.name}"
                # Correcting double forward slashes in 'parent_path'
                subdirectory_path = subdirectory_path.replace('//', '/')


                data_files.extend(self.list_files(endpoint=endpoint, path=subdirectory_path, start_date=start_date, end_date=end_date, should_list_recursively=should_list_recursively, max_depth=max_depth, _current_depth=(_current_depth + 1), filter=filter).DATA)
            elif file_item.type == FilesystemDataType('file') and (not start_date and not end_date or self.is_file_within_date_range(file_item, start_date, end_date)):
                data_files.append(file_item)

        file_list = FileList(
            DATA_TYPE=response_dict['DATA_TYPE'],
            DATA=data_files,
            absolute_path=response_dict['absolute_path'],
            endpoint=response_dict['endpoint'],
            length=len(data_files),
            path=response_dict['path'],
            rename_supported=response_dict['rename_supported'],
            symlink_supported=response_dict['symlink_supported'],
            total=response_dict['total'],
        )
        return file_list
    

    

    def is_file_within_date_range(self, file_item: File, start_date: Optional[str], end_date: Optional[str]) -> bool:
        """
        Check if the file's last modification date falls within the specified start and end dates.

        :param file_item: The file item to check.
        :param start_date: The start date for the search.
        :param end_date: The end date for the search.
        :return: True if the file's last modification date falls within the date range, False otherwise.
        """
        if start_date and file_item.last_modified < start_date:
            return False
        if end_date and file_item.last_modified > end_date:
            return False
        return True
        

    def get_greatlakes_gen_scripts_log_files(self, max_num_day_ago: int = 4, start_date=None):
        """ Gets the log files produced by `gen_scripts` on Greatlakes
        from phoglobushelpers.PhoGlobusHelper import get_greatlakes_gen_scripts_log_files

        all_log_file_df, most_recent_only_log_file_df = get_greatlakes_gen_scripts_log_files(connect_man)
        most_recent_only_log_file_df
        """
        if start_date is None:
            earliest_search_day_date = (datetime.now() - timedelta(days=max_num_day_ago)).date()
            DAY_DATE_STR: str = earliest_search_day_date.strftime("%Y-%m-%d")
            print(f'earliest_search_day_date: {DAY_DATE_STR}')
            start_date = DAY_DATE_STR
            
        lab_Greatlakes_gen_scripts = Bookmark(bookmark_id='99efa634-3ead-11ef-888b-2b3122c1d121', name='Greatlakes gen_scripts', endpoint_id='8c185a84-5c61-4bbc-b12b-11430e20010f', path='/umms-kdiba/Data/Output/gen_scripts/')
        log_file_list: FileList = self.list_files(endpoint=lab_Greatlakes_gen_scripts.endpoint_id, path=lab_Greatlakes_gen_scripts.path, start_date=start_date, end_date=None, should_list_recursively=True, max_depth=1, filter="name:~*.log,~*.err")
        all_log_file_df: pd.DataFrame = log_file_list.to_dataframe() #.columns
        
        most_recent_only_log_file_df = get_only_most_recent_log_files(log_file_df=all_log_file_df)
        return all_log_file_df, most_recent_only_log_file_df
    

    def get_greatlakes_collected_outputs_files(self, max_num_day_ago: int = 4, start_date=None, filter="name:~*.csv,~*.h5"):
        """ Gets the log files produced by `gen_scripts` on Greatlakes
        from phoglobushelpers.PhoGlobusHelper import get_greatlakes_gen_scripts_log_files

        all_log_file_df, most_recent_only_log_file_df = get_greatlakes_gen_scripts_log_files(connect_man)
        most_recent_only_log_file_df
        """
        if start_date is None:
            earliest_search_day_date = (datetime.now() - timedelta(days=max_num_day_ago)).date()
            DAY_DATE_STR: str = earliest_search_day_date.strftime("%Y-%m-%d")
            print(f'earliest_search_day_date: {DAY_DATE_STR}')
            start_date = DAY_DATE_STR
            
        lab_Turbo_collected_outputs_bookmark = Bookmark(bookmark_id='91181e28-f90d-11ee-9222-472b0fe4395a', name='KDIBA Lab Turbo - collected_outputs', endpoint_id='8c185a84-5c61-4bbc-b12b-11430e20010f', path='/umms-kdiba/Data/Output/collected_outputs/')
        lab_Greatlakes_gen_scripts = Bookmark(bookmark_id='99efa634-3ead-11ef-888b-2b3122c1d121', name='Greatlakes gen_scripts', endpoint_id='8c185a84-5c61-4bbc-b12b-11430e20010f', path='/umms-kdiba/Data/Output/gen_scripts/')
        file_list: FileList = self.list_files(endpoint=lab_Turbo_collected_outputs_bookmark.endpoint_id, path=lab_Turbo_collected_outputs_bookmark.path, start_date=start_date, end_date=None, should_list_recursively=True, max_depth=2, filter=filter)
        all_file_df: pd.DataFrame = file_list.to_dataframe() #.columns
        
        # most_recent_only_file_df = get_only_most_recent_log_files(log_file_df=all_file_df)
        return all_file_df


