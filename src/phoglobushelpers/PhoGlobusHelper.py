from datetime import datetime
from typing import Any, List, Optional, Dict
from globus_sdk.response import GlobusHTTPResponse
import pandas as pd
import globus_sdk
from globus_sdk import AccessTokenAuthorizer, TransferClient, TransferData
from globus_sdk.scopes import TransferScopes
from attrs import define, field, Factory

from phoglobushelpers.compatibility_objects.Bookmarks import Bookmark, BookmarkList
from phoglobushelpers.compatibility_objects.Files import File, FilesystemDataType, FileList
from phoglobushelpers.compatibility_objects.Tasks import FatalError, Task, TaskList


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


    def batch_transfer_files(self, source_endpoint:str, destination_endpoint:str, filelist_source:List, filelist_dest:List, max_single_file_wait_time_seconds=3*60*60):
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

            batch_transfer_files(transfer_client, source_endpoint=endpoint_LNX00052_Fedora, destination_endpoint=endpoint_LNX00052_Fedora, filelist_source:List, filelist_dest:List, max_single_file_wait_time_seconds=3*60*60)

        """
        transfer_client: TransferClient = self.transfer_client

        # Set your transfer options
        transfer_options = {
            "preserve_timestamp": True
        }

        # Turn them all into strings
        filelist_source = [str(path) for path in filelist_source]
        filelist_dest = [str(path) for path in filelist_dest]

        # Verify that the source and destination lists have the same length
        assert len(filelist_source) == len(filelist_dest), "Error: Source and destination file lists must have the same length."

        # Loop through the file lists and initiate the transfers
        for source_path, destination_path in zip(filelist_source, filelist_dest):
            # Create a TransferData object
            transfer_data = TransferData(
                transfer_client,
                source_endpoint,
                destination_endpoint,
                label="Batch File Transfer",
                sync_level="checksum",
                **transfer_options
            )

            # Add the file transfer to the TransferData object
            transfer_data.add_item(source_path, destination_path)

            # Initiate the transfer
            transfer_result = transfer_client.submit_transfer(transfer_data)

            print(f"Transferring {source_path} to {destination_path}...")

            # Wait for the transfer to complete
            transfer_client.task_wait(transfer_result["task_id"], timeout=max_single_file_wait_time_seconds, polling_interval=10)

        print("All transfers completed successfully.")


    def list_files(self, endpoint: str, path: str, start_date: Optional[str] = None, end_date: Optional[str] = None, should_list_recursively: bool = False) -> FileList:
        transfer_client: TransferClient = self.transfer_client

        response_dict = transfer_client.operation_ls(
            endpoint,
            path=path,
            orderby=["type", "name"]
        )

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

            if file_item.type == FilesystemDataType('dir'):
                if should_list_recursively:
                    subdirectory_path = f"{path}/{file_item.name}" if path != '/' else f"/{file_item.name}"
                    data_files.extend(self.list_files(endpoint, subdirectory_path, start_date, end_date, should_list_recursively).DATA)
            elif file_item.type == FilesystemDataType('file') and (not start_date and not end_date or self.file_within_date_range(file_item, start_date, end_date)):
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
    

    def file_within_date_range(self, file_item: File, start_date: Optional[str], end_date: Optional[str]) -> bool:
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

        


    