from pathlib import Path
from typing import List, Union
from SSHClientHandler import SSHClientHandler
from scp import SCPClient
import shutil

class FileMover:
    def __init__(self, filepaths: List[Union[str, Path]]):
        self._filepaths = filepaths
        
    @staticmethod
    def _ensure_root(path):
        if isinstance(path, str):
            path = Path(path)
        
        if not isinstance(path, Path):
            raise ValueError(f'path must be a string or a Path object, but is {type(path)}')
        
        path.mkdir(parents=True, exist_ok=True)
        return path
        
    def move_files_locally(self, target_root: Union[str, Path] = Path.cwd() / 'moved_files') -> List[Path]:
        """
        Locally move the files in the filepaths list to a target root directory. 
        The target root directory is created if it does not exist.

        Args:
            target_root (Union[str, Path]): The root directory to move the files to. 
            Defaults to a subdirectory 'moved_files' in the current working directory.

        Returns:
            List[Path]: A list of the new file paths after moving the files.
        """        
        #Ensure we have a valied path to the root and create a dir if it does not exist yet
        target_root = self._ensure_root(target_root)
        #Placeholder new filepaths
        new_filepaths = []
        #Move the files to the target root directory and update the filepaths
        for file in self._filepaths:
            try:
                new_filepaths.append(shutil.move(file, target_root))
            except Exception as e:
                print(f'Error moving file: {e}')
        #Update the filepaths
        self._filepaths = new_filepaths
        return new_filepaths
    

    def migrate_files(self, target_root: Union[str, Path]):
        '''
        Migrate the files in the filepaths list to a remote server using the given credentials.
        
        Args:
            target_root (Union[str, Path]): root directory of the target folder on the remote server (Path object or string)
            hostname (str): hostname of the remote server
            username (str): username to connect to the remote server
            password (str): password to connect to the remote server
            
        Returns:
            List[Path]: A list of the new file paths after moving the files.
        '''
        ssh_handler = SSHClientHandler()
        ssh_handler.connect_to_remote()
        ssh_handler.ensure_remote_dir(target_root)
        ssh_client = ssh_handler.ssh
        #Placeholder new filepaths
        new_filepaths = []
        
        #Migrate the files to the target root directory and update the filepaths
        with SCPClient(ssh_client.get_transport()) as scp:
            for file in self._filepaths:
                try:
                    scp.put(str(file), str(target_root))            #Convert paths to str to ensure SCP compatibility
                    new_filepaths.append(Path(target_root) / Path(file.name))
                except Exception as e:
                    print(f'Error migrating file: {e}')
                    new_filepaths.append(None)
        #Update the filepaths
        self._filepaths = new_filepaths
        ssh_handler.disconnect_from_remote()
        return new_filepaths
    
    @property
    def filepaths(self):
        return self._filepaths
