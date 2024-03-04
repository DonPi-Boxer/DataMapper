import pandas as pd
import yaml
from pathlib import Path
import json
from typing import Union
import numpy as np
import os
import shutil


class MetaDataHandler:
    def __init__(self, metadata : pd.DataFrame, label_map_dict : dict = None, file_col: Union[str,int] = None, label_col: Union[str,int] = None) -> None:
        self.metadata = metadata
        self.unmapped_input_labels = []
        self.label_map_dict = label_map_dict
        self.file_col = file_col
        self.label_col = label_col
        
        try:
            assert (isinstance(file_col, str) and isinstance(label_col, str)) or (isinstance(file_col, int) and isinstance(label_col, int))
            self.file_col = file_col
            self.label_col = label_col
        except:
            raise ValueError(f'file_col and label_col must be of the same type and either string or integer, but are {type(file_col)} and {type(label_col)}')
    
    def get_relevant_metadata(self) -> pd.DataFrame:       #TODO: think of better function name here
        """
        Only keeps the relevant columns (file_path and labels) of the input metadata 
        """
        if isinstance(self.file_col, str):
            self.metadata = self.metadata[[self.file_col, self.label_col]]
        elif isinstance(self.file_col, int):
            self.metadata = self.metadata.iloc[:, [self.file_col, self.label_col]]
        return self.metadata
    
    def set_metadata_columns(self, file_col = 'file', label_col = 'label'):
        self.file_col = file_col
        self.label_col = label_col
        self.metadata.columns = [self.file_col, self.label_col]

    def map_label(self, original_label):
        """
        Function to find the key corresponding to a given value in a dictionary.

        Args:
        original_label (any) : The original label we want to rewrite
        input_dict (dict): The dictionary to search. Is of from {target_label: [source_labels]}

        Returns:
        key: The key corresponding to the given value, or the source label if it is not found in the label mapper
        """
        
        ##Check if self.label_map_dict is a dictionary
        try:
            type(self.label_map_dict) == dict
        except:
            raise ValueError(f'label_map_dict must be a dictionary, but is {type(self.label_map_dict)}')
 
        for target_label, source_labels in self.label_map_dict.items():
            if original_label in source_labels:
                return target_label
        self.unmapped_input_labels.append(original_label)
        return original_label


    def add_root_path(self, root_path : str) -> None:
        """
        This function takes in a root path and adds it to the file paths in the metadata
        """
        self.metadata[self.file_col] = self.metadata[self.file_col].apply(lambda x: root_path + x)

    def relabel_data(self) -> pd.DataFrame:
        """
        This function takes in a dataframe and a dictionary of label mappings and returns a dataframe with the labels replaced
        """        
        if type(self.label_col) == str:
            self.metadata[self.label_col] = self.metadata[self.label_col].apply(lambda x: self.map_label(x))
        elif type(self.label_col) == int:
            self.metadata.iloc[:, self.label_col] = self.metadata.iloc[:, self.label_col].apply(lambda x: self.map_label(x))
    
        print(len(self.unmapped_input_labels))
        if len(self.unmapped_input_labels) > 0:
            # TODO: only display the unique values
            self.unmapped_input_labels = list(set(self.unmapped_input_labels))
            print(f'The following {len(self.unmapped_input_labels)} labels were not found in the label_map_dict: {self.unmapped_input_labels} and saved to unmapped_labels.csv')
            self.save_unmapped_labels()
        else:
            self.unmapped_input_labels = None
            
        return self.metadata
    
    def remove_nan(self):
        self.metadata = self.metadata.dropna()
        
    #TODO: sep argument is save_metadata is not functional right now
            
    
    def save_unmapped_labels(self, file_path : str = None) -> None:
        """
        This function takes in a file path and saves the list of unmapped labels to a file
        """
        if not file_path:
            file_path = 'unmapped_labels.csv'
        pd.Series(self.unmapped_input_labels).to_csv(file_path, index = False)
    
    #TODO: function might be redundant; possibly remove
    def set_label_map_dict(self, label_map_dict):
        self.label_map_dict = label_map_dict

        
    def pick_random_test_files(self, N=10):
        '''
        Takes a (random) subsset of the metdata, where each present subset is of size N
        '''
        # Grouping the DataFrame based on the specified column
        grouped_df = self.metadata.copy().groupby(self.label_col)
        
        # Initialize an empty list to store the randomized DFGroupByObjects
        random_dfs = []
        
        # Loop through each group
        for group_name, group_data in grouped_df:
            # Randomly select N rows from each group
            random_indices = np.random.choice(group_data.index, size=min(N, len(group_data)), replace=False)
            random_dfs.append(group_data.loc[random_indices])
        
        #Concatenate the list of dataframes
        self.metadata = pd.concat(random_dfs)
        # Shuffle the rows in the new DataFrame
        self.metadata = self.metadata.sample(frac=1).reset_index(drop=True)
        
        return self.metadata
    
    def move_test_files(self, target_root_dir):
        '''
        Moves the test files to the target directory. Needed because the DDS pipeline will preprocess ALL present DICOMS in the DICOM_root_dir,
        also if the files are not specified in the label file.
        '''
        if not os.path.exists(target_root_dir):
            os.makedirs(target_root_dir)
        
        #Get list of original file_paths as defines in the random_test_files dataframe
        self.metadata[self.file_col] = self.metadata[self.file_col].apply(lambda x: shutil.move(x, target_root_dir))
        return self.metadata
            
    
#TODO: this function seems unneccecary; possibly remove
def rewrite_label_map(source_to_str_map, target_to_num_map):
    '''
    Maps a 
    source_to_str_map (cq a labels map that maps inconsistent source labels to our defined target string labels) to a 
    target_to_num_map (cq a label map that maps the target string labels to a numeric label) to obtain a
    source_to_num_map (cq a label map that maps the incosnsistent source labels to a numeric label)
    
    Args:
    source_to_target_str_map (dict): The string label map
    target_to_num_map (dict): The numeric label map
    
    Returns:
    source_to_target_numeric_map (dict): The source to target numeric label map
    '''
    source_to_num_map = {}
    for target_str,target_num in target_to_num_map.items():
        if target_str in source_to_str_map.keys():
            source_to_num_map[target_num] = source_to_str_map[target_str]
    return source_to_num_map

       
class Utils:
    @staticmethod
    def load_json(file_path):
        with open(file_path) as f:
            return json.load(f)
    
    @staticmethod
    def remove_row_with_col_keyword(df, col, keyword):
        return df[~df[col].str.contains(keyword)]
    

if __name__ == '__main__':
    source_metadata_path = '/home/donpi-boxer/BME/Thesis/repos/data/UPEN_GMB/manifest-1669766397961/metadata.csv'
    source_metadata_df = pd.read_csv(source_metadata_path)
    file_col = 'File Location'
    label_col = 'Series Description'
    dicom_root = '/home/donpi-boxer/BME/Thesis/repos/data/Prostate-MRI/manifest-1694710246744/Prostate-MRI-US-Biopsy'
    
    source_to_str_mapper_path = 'source_to_target_string_mapper.json'
    source_to_str_mapper = Utils.load_json(source_to_str_mapper_path)
    
    target_to_num_mapper_path = 'labelMap_no_binning.json'
    target_to_num_mapper = Utils.load_json(target_to_num_mapper_path)
    
    labelmapper = rewrite_label_map(source_to_str_mapper, target_to_num_mapper)
    
    mapper = MetaDataHandler(metadata = source_metadata_df, label_map_dict = labelmapper, file_col = file_col, label_col = label_col)
    source_metadata = mapper.get_relevant_metadata()
    mapper.set_metadata_columns()
    mapper.remove_nan()
    mapper.relabel_data()
    mapper.add_root_path(dicom_root)
    test_data = mapper.pick_random_test_files(5)
    mapper.move_test_files(target_root_dir=dicom_root + '/test_data')
    test_data.to_csv('test_data.tsv', index = False, sep='\t', lineterminator='\n', header=False)
    
    