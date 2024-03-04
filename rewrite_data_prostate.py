from MetaDataHandler import *
import pandas as pd

if __name__ == '__main__':
    #Static arguments (cq those we do not have to change for different datasets)
    source_to_str_mapper_path = 'stringlabel_mapper.json'
    file_path_col = 'File Location'
    label_col = 'Series Description'
    target_to_num_mapper_path = 'labelMap_no_binning.json'
    
    #Arguments we have to change for different datasets
    source_metadata_path = '/home/donpi-boxer/BME/Thesis/repos/data/Prostate-MRI/manifest-1694710246744/metadata.csv'
    dicom_root = '/home/donpi-boxer/BME/Thesis/repos/data/Prostate-MRI/manifest-1694710246744/'
    
    #Constant that will be loaded in all datasets
    source_metadata_df = pd.read_csv(source_metadata_path)
    source_to_str_mapper = Utils.load_json(source_to_str_mapper_path)
    
    source_metadata_df = source_metadata_df[~source_metadata_df['Modality'].str.contains('US')]
    target_to_num_mapper = Utils.load_json(target_to_num_mapper_path)    
    labelmapper = rewrite_label_map(source_to_str_mapper, target_to_num_mapper)
    
    
    mapper = MetaDataHandler(metadata = source_metadata_df, label_map_dict = labelmapper, file_col = file_path_col, label_col = label_col)
    source_metadata = mapper.get_relevant_metadata()
    mapper.set_metadata_columns()
    mapper.remove_nan()
    mapper.relabel_data()
    if not mapper.unmapped_input_labels:
        print('hoi')
        mapper.add_root_path(dicom_root)
        mapper.move_test_files(target_root_dir=dicom_root + '/test_data')
        test_data.to_csv('test_data.tsv', index = False, sep='\t', lineterminator='\n', header=False)
    
    