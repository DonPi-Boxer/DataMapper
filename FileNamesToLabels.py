import pandas as pd
from pathlib import Path
from typing import Union, Dict, List
from Utils import Utils

class FileNamesToLabels:
    def __init__(self, 
                 root_dir: Union[Path, str], 
                 file_extension: str,
                 suffix_to_label_map: Dict[str,str],
                 suffix_sep: str = '_') -> None:
        
        self.root_dir = Path(root_dir)
        self.file_extension = file_extension
        self.suffix_to_label_map = suffix_to_label_map
        self.suffix_sep = suffix_sep
        self._labeldf = None
          
    def filenames_to_labels_df(self) -> list:
        """
        Gets all file paths with a specific file extension from a directory structure.
        
        Returns:
        pd.DataFrame: A DataFrame containing file paths.
        """
        # Initialize a list to store file paths
        labeldict = {'file_path': [], 'suffix': [], 'label': []}
        # Search for files with the specified file extension
        for file_path in self.root_dir.rglob(f'*.{self.file_extension}'):
            print(f'file_path: {file_path}')
            suffix = file_path.name.split('.')[-2].split(self.suffix_sep)[-1]
            print(f'suffix: {suffix}')
            label = self.suffix_to_label_map.get(suffix, None)
            print(f'label: {label}')
            if label is not None:
                labeldict['file_path'].append(str(file_path))
                labeldict['suffix'].append(suffix)
                labeldict['label'].append(label)       
        self._labeldf = pd.DataFrame(labeldict)
        return self._labeldf
    
    def save_labels_csv(self, 
                        cols_to_save: List[str] = ['file_path', 'label'], 
                        output_csv: str = 'labels.csv') -> None:
        """
        Writes the file paths and labels to a CSV file.
        
        Args:
        cols_to_save (list): The columns to save to the CSV file.
        output_csv (str): The path to the output CSV file.
        """
        print('hi')
        return Utils.save_labels_csv(labeldf=self.labeldf, cols_to_save=cols_to_save, output_csv=output_csv)
        
    @property
    def labeldf(self) -> pd.DataFrame:
        if self._labeldf is None:
            self.filenames_to_labels_df()
        return self._labeldf
        
# Example usage
if __name__ == '__main__':
    root = '/home/donpi-boxer/BME/Thesis/data/prostate/PI-CAI Challenge'
    #TODO: write code snippet that automatically generate numeric labels from string labels and also returns the string_to_num labelmap
    labelmap = {'t2w': 0, 'hbv': 1, 'adc': 2}
    labeler = FileNamesToLabels(root_dir = root, file_extension='mha', suffix_to_label_map=labelmap)
    labeldf = labeler.labeldf
    out = labeler.save_labels_csv(output_csv='labels.csv')                                              #Get the label map
    Utils.write_json('labelmap.json', labelmap)
    
    
    
    