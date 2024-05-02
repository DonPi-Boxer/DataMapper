import json
from dotenv import load_dotenv
import os
import pandas as pd
from typing import List

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
    def get_ssh_credentials():
        """Retrieve SSH credentials from environment variables"""
        hostname = os.getenv('SSG_HOSTNAME')
        
    @staticmethod
    def save_labels_csv(labeldf: pd.DataFrame,
                        cols_to_save: List[str] = ['file_path', 'label'], 
                        output_csv: str = 'labels.tsv') -> None:
        """
        Writes the file paths and labels to a CSV file.

        Args:
        cols_to_save (list): The columns to save to the CSV file.
        output_csv (str): The path to the output CSV file.
        """
        labeldf[cols_to_save].to_csv(output_csv, index=False, sep='\t', lineterminator='\n', header=False)
        return output_csv