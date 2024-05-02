from pathlib import Path
from typing import Union, List, Dict, Optional
import pandas as pd
from Utils import *


class LabelMapper:
    def __init__(self,
                 acronym_dir: Optional[Union[str, Path]] = Path.cwd() / 'acronyms') -> None:
        """
        Initializes the DataMapper with the directory containing substring .txt files.

        Args:
            labels_to_map (List[str]): The list of labels to map.
            root_dir (str): The directory path where .txt files are stored.
        """
        self._acronym_dir = Path(acronym_dir)
        self._relative_map = self.load_mapping_dict_from_root(self._acronym_dir / 'relative')
        self.absolute_map = self.load_mapping_dict_from_root(self._acronym_dir / 'absolute')
        self._unmapped_labels = []
        self._succesful_relative_maps = {}
        self._ambigous_relative_maps = {}
        
        
    @staticmethod
    def load_mapping_dict_from_root(root_dir: Union[str, Path]) -> Dict[str, List[str]]:
        """
        Loads all .txt files in the specified directory and creates a dictionary of substrings.
        The filename stem will be the dictionary key.

        Returns:
            dict: A dictionary with keys as modality names and values as lists of substrings.
        """
        substrings_dict = {}
        for file_path in root_dir.iterdir():
            if file_path.suffix == '.txt':
                modality = file_path.stem
                with file_path.open('r') as file:
                    substrings_dict[modality] = [line.strip() for line in file.readlines()]
        return substrings_dict

    def _relative_mapping(self, label: str) -> str:
        """
        Maps a label to a main MRI modality based on substrings. Returns None if no match found.
        The comparison is case-insensitive.

        Args:
            label (str): The original label from the dataset.

        Returns:
            str: The mapped main MRI modality or None if no substrings match.
        """
        label_lower = label.lower()                                                     #Convert the label to lowercase to ensure case-insensitive comparison
        for main_modality, substrings in self._map_dict.items():
            if any(substring.lower() in label_lower for substring in substrings):
                self._record_succesful_mapping(main_modality) #Record the succesful mapping
                return main_modality
        self._record_unsuccesful_mapping(label)               #Record the unsuccesful mapping
        return None  # Return None for unmapped labels

    def _absolute_mapping(self, label:str) -> str:
        """
        Maps a label to a main MRI modality based on exact match. Returns None if no match found.
        The comparison is case-insensitive.

        Args:
            label (str): The original label from the dataset.

        Returns:
            str: The mapped main MRI modality or None if no exact match found.
        """
        label_lower = label.lower()                           #Convert the label to lowercase to ensure case-insensitive comparison
        for main_modality, substrings in self._map_dict.items():
            if label_lower in substrings:
                return main_modality
        self._record_unsuccesful_mapping(label)               #Record the unsuccesful mapping
        return None  # Return None for unmapped labels
    
    def map_labels(self, labels_to_map: List[str]) -> List[str]:
        """
        Maps a list of labels to main modalities based on substrings.

        Args:
            labels (List[str]): The list of original labels from the dataset.

        Returns:
            List[str]: The list of mapped main MRI modalities.
        """
        
        self._unmapped_labels.clear()
        mapped_labels = [self._map_label(label) for label in labels_to_map]
        if len(self._unmapped_labels) > 0:
            print(f"WARNING: {len(self._unmapped_labels)} labels could not be mapped! See unmapped_labels.txt for details.")
            self.save_unmapped_labels()
            self.save_succesful_mapping_dict()
        else:
            print('All labels successfully mapped!')
        return mapped_labels
        
    def _record_succesful_mapping(self, main_modality: str, old_label: str) -> str:
        if main_modality not in self._succesful_mappings:
            self._succesful_mappings[main_modality] = []
        if old_label not in self._succesful_mappings[main_modality]:
            self._succesful_mappings[main_modality].append(old_label)
        
    def _record_unsuccesful_mapping(self, old_label: str) -> str:
        if old_label not in self._unmapped_labels:
            self._unmapped_labels.append(old_label)
            
    def get_unique_mapped_labels(self):
        return list(self._succesful_mappings.keys()) 
              
    @property
    def unmapped_labels(self):
        return self._unmapped_labels
    
    @property
    def succesful_mappings(self):
        return self._succesful_mappings
    
    def save_unmapped_labels(self, file_path: Union[str, Path] = Path.cwd() / 'unmapped_labels.txt') -> str:
        pd.Series(self._unmapped_labels).to_csv(file_path, index = False, header=False, sep=' ')
        return file_path

    def save_succesful_mapping_dict(self, file_path: Union[str, Path] = Path.cwd() / 'succesful_mappings.json') -> str:
        Utils.write_json(file_path, self._succesful_mappings)
        return file_path

