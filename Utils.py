import json
from dotenv import load_dotenv
import os

class Utils:
    
    @staticmethod
    def load_json(file_path):
        with open(file_path) as f:
            return json.load(f)
    
    @staticmethod
    def write_json(file_path, file_to_write):
        with open(file_path, 'w') as file:
            json.dump(file_to_write, file, indent=4)
    
    @staticmethod
    def get_ssg_credentials():
        """Retrieve SSH credentials from environment variables"""
        hostname = os.getenv('SSG_HOSTNAME')