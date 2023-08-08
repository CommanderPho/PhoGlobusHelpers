import os
import sys
import platform
from contextlib import contextmanager
import pathlib
from pathlib import Path
from typing import List, Optional
import shutil # for _backup_extant_file(...)
from datetime import datetime
import pandas as pd


known_global_data_root_parent_paths = [Path(r'W:\Data'), Path(r'/media/MAX/Data'), Path(r'/Volumes/MoverNew/data'), Path(r'/home/halechr/turbo/Data'), Path(r'/nfs/turbo/umms-kdiba/Data')]


def find_first_extant_path(path_list: List[Path]) -> Path:
    """Returns the first path in the list that exists.
    from pyphocorehelpers.Filesystem.path_helpers import find_first_extant_path

    Args:
        path_list (List[Path]): _description_

    Raises:
        FileNotFoundError: _description_

    Returns:
        Path: _description_
    """
    for a_path in path_list:
        if a_path.exists():
            return a_path
    raise FileNotFoundError(f"Could not find any of the paths in the list: {path_list}")


def discover_data_files(basedir: Path, glob_pattern='*.mat', recursive=True):
    """ By default it attempts to find the all *.mat files in the root of this basedir
    Example:
        basedir: Path(r'~/data/Bapun/Day5TwoNovel')
        session_name: 'RatS-Day5TwoNovel-2020-12-04_07-55-09'

    Example 2:
        from pyphocorehelpers.Filesystem.path_helpers import discover_data_files
        
        search_parent_path = Path(r'W:\\Data\\Kdiba')
        found_autoversioned_session_pickle_files = discover_data_files(search_parent_path, glob_pattern='*-loadedSessPickle.pkl', recursive=True)
        found_global_computation_results_files = discover_data_files(search_parent_path, glob_pattern='output/*.pkl', recursive=True)

        found_files = found_global_computation_results_files + found_autoversioned_session_pickle_files
        found_files

    """
    if isinstance(basedir, str):
        basedir = Path(basedir) # convert to Path object if not already one.
    if recursive:
        glob_pattern = f"**/{glob_pattern}"
    else:
        glob_pattern = f"{glob_pattern}"
    found_files = sorted(basedir.glob(glob_pattern))
    return found_files # 'RatS-Day5TwoNovel-2020-12-04_07-55-09'



def print_data_files_list_as_array(filenames_list):
    """ tries to print the output list of data files from `discover_data_files` as a printable array. 
    
    Usage:
        from pyphocorehelpers.Filesystem.path_helpers import discover_data_files
        
        print_data_files_list_as_array(found_files)
    """
    # print(f'filenames_list: {filenames_list}')
    # filenames_list_str = ',\n'.join([str(a_path) for a_path in filenames_list])
    filenames_list_str = ',\n'.join([f'r"{str(a_path)}"' for a_path in filenames_list])
    print(f'filenames_list_str: [{filenames_list_str}]')
    # for a_path in filenames_list:
    #     print(f'{str(a_path)}')


def get_file_metadata(paths) -> pd.DataFrame:
    """
    Get the metadata (modification time, creation time, and file size) for each file specified by a list of pathlib.Path objects.
    :param paths: A list of pathlib.Path objects representing the file paths.
    :return: A pandas DataFrame with columns for path, modification time, creation time, and file size.
    """
    metadata = []

    for path in paths:
        if path.is_file():
            modified_time = os.path.getmtime(path)
            created_time = os.path.getctime(path)
            file_size = os.path.getsize(path) / (1024 ** 3)  # Convert to GB
            metadata.append({
                'path': str(path),
                'modification_time': datetime.fromtimestamp(modified_time),
                'creation_time': datetime.fromtimestamp(created_time),
                'file_size': file_size
            })

    df = pd.DataFrame(metadata)
    return df



class FileList:
    """ helpers for manipulating lists of files.
    Usage:
        from pyphocorehelpers.Filesystem.path_helpers import FileList
    """
    @staticmethod
    def excluding_pattern(paths, exclusion_pattern):
        return [str(path) for path in paths if not str(path).match(exclusion_pattern)]

    @staticmethod
    def from_set(*args) -> list[list[Path]]:
        if len(args) == 1:
            return [[Path(path) for path in a_list] for a_list in args][0]
        else:
            return [[Path(path) for path in a_list] for a_list in args]
        
    @staticmethod
    def to_set(*args) -> list[set[str]]:
        if len(args) == 1:
            return [set(str(path) for path in a_list) for a_list in args][0] # get the item so a raw `set` is returned instead of a list[set] with a single item
        else:
            return [set(str(path) for path in a_list) for a_list in args]

    @classmethod
    def subtract(cls, lhs, rhs) -> list[Path]:
        """ 
        
        Example:
            non_primary_desired_files = FileList.subtract(found_any_pickle_files, (found_default_session_pickle_files + found_global_computation_results_files))
        
        """
        return cls.from_set(cls.to_set(lhs) - cls.to_set(rhs))




def convert_filelist_to_new_parent(filelist_source: List[Path], original_parent_path: Path = Path(r'/media/MAX/cloud/turbo/Data'), dest_parent_path: Path = Path(r'/media/MAX/Data')) -> List[Path]:
    """ Converts a list of file paths from their current parent, specified by `original_parent_path`, to their new parent `dest_parent_path` 

    Usage:
        from pyphocorehelpers.Filesystem.path_helpers import convert_filelist_to_new_parent
        source_parent_path = Path(r'/media/MAX/cloud/turbo/Data')
        dest_parent_path = Path(r'/media/MAX/Data')
        # # Build the destination filelist from the source_filelist and the two paths:
        filelist_dest = convert_filelist_to_new_parent(filelist_source, original_parent_path=source_parent_path, dest_parent_path=dest_parent_path)
        filelist_dest
    """
    if original_parent_path.resolve() == dest_parent_path.resolve():
        print('WARNING: convert_filelist_to_new_parent(...): no difference between original_parent_path and dest_parent_path.')
        return filelist_source
    else:
        filelist_dest = []
        for path in filelist_source:
            relative_path = str(path.relative_to(original_parent_path))
            new_path = Path(dest_parent_path) / relative_path
            filelist_dest.append(new_path)
        return filelist_dest

def find_matching_parent_path(known_paths: List[Path], target_path: Path) -> Optional[Path]:
    """ returns the matching parent path in known_paths that is a parent of target_path, otherwise returns None if no match is found. 

    Usage:
        from pyphocorehelpers.Filesystem.path_helpers import find_matching_parent_path

        known_global_data_root_parent_paths = [Path(r'W:\Data'), Path(r'/media/MAX/Data'), Path(r'/Volumes/MoverNew/data'), Path(r'/home/halechr/turbo/Data'), Path(r'/nfs/turbo/umms-kdiba/Data')]
        prev_global_data_root_parent_path = find_matching_parent_path(known_global_data_root_parent_paths, curr_filelist[0]) # TODO: assumes all have the same root, which is a valid assumption so far. ## prev_global_data_root_parent_path should contain the matching path from the list.
        assert prev_global_data_root_parent_path is not None, f"No matching root parent path could be found!!"
        new_session_batch_basedirs = convert_filelist_to_new_parent(curr_filelist, original_parent_path=prev_global_data_root_parent_path, dest_parent_path=desired_global_data_root_parent_path)
    """
    target_path = target_path.resolve()
    for path in known_paths:
        if target_path.is_relative_to(path.resolve()):
            return path
    return None



@contextmanager
def set_posix_windows():
    """ Prevents errors unpickling POSIX Paths on Windows that were previously pickled on Unix/Linux systems
    
    Usage:
        from pyphocorehelpers.Filesystem.path_helpers import set_posix_windows
        with set_posix_windows():
            global_batch_run = BatchRun.try_init_from_file(global_data_root_parent_path, active_global_batch_result_filename=active_global_batch_result_filename,
                                    skip_root_path_conversion=True, debug_print=debug_print) # on_needs_create_callback_fn=run_diba_batch
    """
    if platform.system() == 'Windows':
        posix_backup = pathlib.PosixPath
        try:
            pathlib.PosixPath = pathlib.WindowsPath
            yield
        finally:
            pathlib.PosixPath = posix_backup
            


def read_lines_from_file(filename):
    with open(filename, 'r') as file:
        lines = [line.strip() for line in file.readlines()]
    return lines
