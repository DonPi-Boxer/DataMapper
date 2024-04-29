import pandas as pd
from typing import Union, List, Optional
from pathlib import Path
from FileMover import FileMover

###TODO: infer the new range of our classes (0, N-1) with N = #classes from the input !
class MetaDataFrameHandler:
    """A class to manipulate and process metadata for datasets."""
    def __init__(self, 
                 source: Union[str, Path, pd.DataFrame], 
                 col_path: Union[str, Path]= 'File Location', 
                 col_label: Union[str, Path]= 'Series Description',
                 col_uid: Union[str, Path]= 'Series UID'
                 ) -> None:
        
        """
        Initialize the MetaData object with source data and relevant columns.

        :param source: Either a path to a CSV file, a Path object, or a pandas DataFrame.
        :param col_path: The name of the column with file paths.
        :param col_label: The name of the column with labels.
        :param col_uid: The name of the column with unique identifiers.
        """
        
        #Load the source data
        self._source_metadata = self.load_source(source)
        #Create copy of the source_df, which will be altered throughout the class; allows us to always revert back to the original source_df
        self._processed_metadata = self._source_metadata.copy()
        #Define the, for our use, most relevant column in the metadata
        self.col_path = col_path
        self.col_label = col_label
        self.col_uid = col_uid
        
    @staticmethod 
    def load_source(source):
        try:
            # If we're given a path for the source, create the source dataframe
            if isinstance(source, (str, Path)):  
                source_metadata = pd.read_csv(source)
            # If we're given a DataFrame for the source, use that as source
            elif isinstance(source, pd.DataFrame):
                source_metadata = source
            else:
                raise ValueError(f'source must be a string, a Path object or a DataFrame, not {type(source).__name__}')
        except Exception as e:
            raise ValueError(f'Failed to load source with error: {str(e)}')
        return source_metadata
        
    @staticmethod
    def _get_cols(df, cols_to_get: list = None) -> pd.DataFrame:
        """Retrieve specific columns from the DataFrame If cols_to_get is none, return the entire dataframe.
        
        Args:
            df (pd.DataFrame) : Pandas dataframe object
            cols_to_get (List)
            """
        return df[cols_to_get] if cols_to_get else df

    def exclude_by_key(self, col: str, key: str) -> pd.DataFrame:
        """
        Exclude rows where the column matches the key
        
        :param col: column header to check
        :param key: key to exclude
        """
        
        self._processed_metadata = self._processed_metadata[~self._processed_metadata[col] == key]
        return self._processed_metadata

    def filter_by_key(self, col: str,key: str):
        """
        Filter and only keep rows where the column matches the key
        
        :param col: header of column to check
        :param key: key to filter
        """
        
        self._processed_metadata = self._processed_metadata[self._processed_metadata[col] == key]
        return self._processed_metadata
                         
    def remove_nan(self, cols: Optional[List[str]]= None) -> pd.DataFrame:
        """
        Remove rows where any specified column or any column contains NaN values, 
        depending on whether column names are provided.

        Args:
            cols (Optional[List[str]]): List of column names to check for NaN values. If None, checks all columns.

        Returns:
            pd.DataFrame: The processed metadata DataFrame with rows containing NaNs in specified columns or any column removed.
        """
        if cols:
             self._processed_metadata.dropna(subset=cols, inplace=True)
        else:
            self._processed_metadata.dropna(inplace=True) 
        return self._processed_metadata
        
    def keep_columns(self, cols: List[str]) -> pd.DataFrame:
        """Keep only the specified column in the processed metadata

        Args:
            cols (List[str]): string headers of the column we want to keep

        Returns:
            pd.DataFrame: Altered state of _processed_metadata with only the specified columns
        """
        self._processed_metadata = self._get_cols(self._processed_metadata, cols)
        return self._processed_metadata
    
    def rename_columns(self, col_dict: dict[str,str]) -> pd.DataFrame:
        """Rename the column headers of the processed metadata according to the provided dictionary

        Args:
            col_dict (Dict[str,str]): Column mapping dictionary of the form {old_header : new_header}, where old_header is
            the current column name and 'new_header' is the new name to assign 

        Returns:
            pd.DataFrame: Altered state of _processed_metadata with the new column names
        """
        self._processed_metadata.rename(columns=col_dict, inplace=True)
        return self._processed_metadata
    
    def add_root_to_path(self, root: Optional[Union[str, Path]] = Path.cwd()) -> pd.DataFrame:
        """
        Add the root path to each file path in the corresponding column of the processed metadata.
        Default is the current directory.

        Args:
            root (Union[str, Path, None]): The base path to prepend to each path in the column.
                                            If None, uses the current directory.

        Raises:
            ValueError: If root is neither a string, a Path object, nor None.

        Returns:
            pd.DataFrame: The updated metadata DataFrame with full paths in the file path column.
        """
        if isinstance(root,str):
                root = Path(root)
        if not isinstance(root, Path):
            raise ValueError(f'root must be a string or a Path object, but is {type(root)}')
        
        self._processed_metadata[self.col_path] = self._processed_metadata[self.col_path].apply(lambda x: root /  x)
        return self._processed_metadata


    def move_processed_metadata_files(self, new_root: Optional[Union[str, Path]] = Path.cwd()/ 'chosen_files') -> pd.DataFrame:
        '''
        Locally moves the files in the processed metadata to a new location, which defaults to a (new) subdir chosen_files in the current working directory.
        Updates the filepaths in the processed metedata to the new location.
        
        Args:
            new_root: root directory of the target folder. Defaults to (new) subdir 'chosen_files' in the current working directory
        
        Returns:
            Pd.DataFrame: dataframe with the file paths updated to the new location. Also perform the move operation self
        '''
        #Move the files to the new root directory and update the file paths in the metadata
        file_mover = FileMover(self._processed_metadata[self.col_path])
        self._processed_metadata[self.col_path] = file_mover.move_files_locally(new_root)
        return self._processed_metadata
     
    def migrate_processed_metadata_files(self, new_root: Union[Path, str]) -> pd.DataFrame:
        '''
        Migrate the files in the processed metadata to a remote server. Host of the remote server can be provided as 
        SSH_HOSTNAME in a .env file; user will be prompted for remote server if SSH_HOSTNAME is not specified in a .env file.
        Username and password will be prompted for.
        
        Args:
            new_root (Union[Path, str]): root directory of the target folder on the remote server (Path object or string)

        Returns:
            Pd.DataFrame: dataframe with the file paths updated to the new location. Also perform the move operation self
        '''
        self.processed_metadata[self.col_path] = self.processed_metadata[self.col_path].apply(lambda x: FileMover.migrate_files(x, new_root))
    
    def view_processed_metadata_columns(self, cols_to_keep: List[str]) -> pd.DataFrame:
        """Return only the specified columns of the processed metadata; does not change the processed metadata.

        Args:
            cols_to_keep: (list[str]): list of the column headers to keep

        Returns:
            pd.DataFrame: processed metadata with only the specified columns
        """
        return self._get_cols(self._processed_metadata.copy(), cols_to_get=cols_to_keep)
    
    @property
    def processed_metadata(self) -> pd.DataFrame:
        """Return the current state of the processed metadata

        Returns:
            pd.DataFrame: The current state of the processed metadata
        """
        return self._processed_metadata.copy() 
        
    def save_metadata_csv(self, 
                          csv_path: Optional[Union[Path, str]] = 'metadata_processed.csv', 
                          cols_to_keep: Optional[List[str]] = None,
                          sep: Optional[str] = ',',
                          headers: Optional[bool] = False,
                          index: Optional[bool] = False) -> str:
        
        """
        Saves specified columns of the processed metadata to a CSV file.

        Args:
        csv_path (Optional[Union[str, Path]]): The file path where the CSV will be saved. Defaults to 'metadata_processed.csv'.
        cols_to_keep (Optional[List[str]]): List of column names to keep in the DataFrame that is saved. 
                                            Defualts to None, meaning all columns are saved.
        sep (str): Delimiter to use in the CSV file. Defaults to ','.
        headers (bool): Whether to write headers in the CSV file. Defaults to False.
        index (bool): Whether to include row indices in the CSV file. Defaults to False.

        Returns:
        str: The path to the saved CSV file.
        """
        if isinstance(csv_path, str):
            csv_path = Path(csv_path)
        if not isinstance(csv_path, Path):
            raise ValueError(f'csv_path must be a string or a Path object, but is {type(csv_path)}')
        
        to_save = self._get_cols(self._processed_metadata.copy(), cols_to_get=cols_to_keep)
        to_save.to_csv(csv_path, sep=sep, header=headers, index=index)
        return csv_path
    
    def revert_to_original_metadata(self) -> pd.DataFrame:
        """Revert the processed metadata to the original metadata

        Returns:
            pd.DataFrame: The original metadata
        """
        self._processed_metadata = self._source_metadata.copy()
        return self._processed_metadata
    
    # def pick_random_test_files(self, N: int):
    #     '''
    #     Takes a (random) subsset of the metdata, where each present subset is of size N
    #     '''
    #     # Grouping the DataFrame based on the specified column
    #     grouped_df = self.metadata.copy().groupby(self.label_col)
        
    #     # Initialize an empty list to store the randomized DFGroupByObjects
    #     random_dfs = []
        
    #     # Loop through each group
    #     for group_name, group_data in grouped_df:
    #         # Randomly select N rows from each group
    #         random_indices = np.random.choice(group_data.index, size=min(N, len(group_data)), replace=False)
    #         random_dfs.append(group_data.loc[random_indices])
        
    #     #Concatenate the list of dataframes
    #     self.metadata = pd.concat(random_dfs)
    #     # Shuffle the rows in the new DataFrame
    #     self.metadata = self.metadata.sample(frac=1).reset_index(drop=True)
        
    #     return self.metadata
            
