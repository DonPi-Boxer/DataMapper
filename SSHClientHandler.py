import os
from paramiko import SSHClient, RejectPolicy
import getpass
from dotenv import load_dotenv
import sys


#TODO: create new class with these three functions beneath ?
class SSHClientHandler(SSHClient):
    def __init__(self) -> None:
        super().__init__()      #Initialize the SSHClient
        self._ssh = None        #Initialize the SSH connection as None
        
        #TODO /#FIXME : not sure if we want to connect on initialization ? Maybe just let the user connect when he wants to ?
        #We make a connection on initialization of this class
        
    @staticmethod
    def _get_hostname() -> str:
        """Retrieves the hostname from a .env file or prompts the user if not available."""
        load_dotenv()                              #Load the .env file
        hostname = os.getenv('REMOTE_HOSTNAME')    #Read the hostname from the .env file
        
        #If no hostname is found (either no .env or no HOSTNAME in existing .env) -> prompt the user for the hostname
        if not hostname:                           
            print("No hostname found")
            hostname = input('Enter the hostname of the remote server: ')
            
        print(f"Connecting to {hostname}")         #Confirm hostname
        return hostname
    
    @staticmethod
    def _get_username() -> str:
        """Prompts the user for the username"""
        return input('Username: ')                
        
    @staticmethod
    def _get_password() -> str:
        """Prompts the user for the password. Password not visible during input."""
        return getpass.getpass('Password: ')
        
    def connect_to_remote(self):
        """Connect to the remote server using the SSH credentials"""
        if not self._ssh:
            self._ssh = SSHClient()
            self._ssh.load_system_host_keys()
            self._ssh.set_missing_host_key_policy(RejectPolicy())    # Set policy to reject if the host key is unknown
        try:
            self._ssh.connect(self._get_hostname(), username=self._get_username(), password=self._get_password())
            print("Succesfuly connected to remote")
        except Exception as e:
            print(f'Error connecting to remote server: {e}')

    def disconnect_from_remote(self):
        """Disconnect from the remote server"""
        if self._ssh:
            self._ssh.close()
            print("Disconnected from remote")
            
    def ensure_remote_dir(self, directory: str):
        #NOTE: fully chatgpt generated....
        """
        Ensure that the directory exists on the remote server.
        If the directory does not exist, ask the user to create it.

        Args:
            directory (str): The path to the directory on the remote server.
        """
        # Check if the directory exists
        stdin, stdout, stderr = self.ssh.exec_command(f"if [ -d '{directory}' ]; then echo 'exists'; fi")
        stdout.channel.recv_exit_status()  # Wait for the command to complete
        result = stdout.read().decode().strip()
        
        # If the directory does not exist, ask the user if it should be created
        if result != "exists":
            response = input(f"Remote dir does not exist. Do you want me to create \n {directory}? (y/n): ")
            if response.lower() == 'y':
                # Create the directory
                stdin, stdout, stderr = self.ssh.exec_command(f"mkdir -p {directory}")
                stdout.channel.recv_exit_status()  # Wait for the command to complete
                if stderr.read():
                    raise Exception(f"Failed to create directory {directory}")
                print(f"Directory {directory} created successfully.")
            else:
                print("Operation aborted by user.")
                sys.exit(1)  # Exit the program if the user decides not to create the directory
                
    @property
    def ssh(self) -> SSHClient:
        """Provides the SSH client instance for use with operations like SCP"""
        if self._ssh is None:
            raise ValueError("No SSH connection established. Please connect first.")
        return self._ssh