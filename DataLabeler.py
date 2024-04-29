import pandas as pd
from pathlib import Path
from typing import Union, List, Dict

class DataLabeler:
    def __init__(self, original_labels: List[str]):
        self._original_labels = original_labels
        self._new_labels = []
        self._unmapped_labels = []
        
                
    def map_labels(self, mapping_dict: Dict[str, str]):
        """
        Maps the original labels to the mapping dict that is given as input. 

        Args:
        input_dict (Dict[str,str]): The dictionary to search. Is of form {target_label: [source_labels]}

        Returns:
        key: The key corresponding to the given value, or the source label if it is not found in the label mapper
        """
        for original_label in self._original_labels:
            new_label = mapping_dict.get(original_label)
            self._new_labels.append(new_label)
            if new_label is None:
                self._unmapped_labels.append(original_label)
        return self._new_labels, self._unmapped_labels
    
    # ###todo: we are here !
    # def rewrite_label_map(source_to_str_map, target_to_num_map):
    #     '''
    #     Maps a 
    #     source_to_str_map (cq a labels map that maps inconsistent source labels to our defined target string labels) to a 
    #     target_to_num_map (cq a label map that maps the target string labels to a numeric label) to obtain a
    #     source_to_num_map (cq a label map that maps the incosnsistent source labels to a numeric label)
        
    #     Args:
    #     source_to_target_str_map (dict): The string label map
    #     target_to_num_map (dict): The numeric label map
        
    #     Returns:
    #     source_to_target_numeric_map (dict): The source to target numeric label map
    #     '''
    #     source_to_num_map = {}
    #     for target_str,target_num in target_to_num_map.items():
    #         if target_str in source_to_str_map.keys():
    #             source_to_num_map[target_num] = source_to_str_map[target_str]
    #     return source_to_num_map
    
    @property
    def new_labels(self):
        return self._new_labels
    
    @property
    def unmapped_labels(self):
        return self._unmapped_labels
    
    def save_unmapped_labels(self, file_path: Union[str, Path] = None):
        if not file_path:
            file_path = 'unmapped_labels.csv'
        pd.Series(self._unmapped_labels).to_csv(file_path, index = False)
        return file_path
    
