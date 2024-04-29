import pandas as pd
from MetaDataFrameHandler import *
from Utils import *
###User input
##Define relevant paths
metadata_file =  '/home/donpi-boxer/BME/Thesis/data/kidney/manifest-xxn3N2Qq630907925598003437/metadata.csv'    #Csv containig the metadata
root_source_dicom = '/home/donpi-boxer/BME/Thesis/data/kidney/manifest-xxn3N2Qq630907925598003437/TCGA-KIRC'    #Root of the dicoms locally   
root_gpu_cluster = '/data/scratch/r393017/data_for_dds/prostate/test_data'                                      #Remote root we will move the data to 


metadata = MetaDataFrameHandler(source= metadata_file)              #Initialize an instance of the MetaDataHandler class
metadata.filter_by_key(col='Modality', key='MR')                    #Only keep the MR modalities 
metadata.remove_nan(cols=['Series Description'])                    #Remove rows where the series description is NaN
metadata.add_root_to_path(root=root_source_dicom)                   #Add the (local) root to the file paths so we can access the files
metadata.save_metadata_csv(headers=True)
metadata.move_processed_metadata_files()                 #Move the files to the remote cluster



###TODO:
#rewrite the labelmapper. Do not make absoulute maps, but if -contains- .... then we map the value !!!


#Rewrite and check the labels
#Upload all the files with the metadata to the GPU cluster



# ###User input
# ##Define needed paths
# #Source data
# root_source = '/data/scratch/r393017/data_for_dds/prostate/test_data'    #Root directory of ALL source data (metadata and DICOMs)                                               
# metadata_source = '/data/scratch/r393017/data_for_dds/prostate/test_data/metadata.csv'   #metadata file as given by the source  

# #Label files
# #TODO: think of better variable names for the two metadatas
# source_to_str_metadata = '/data/scratch/r393017/Datametadata_new/stringlabel_metadata.json'                               #Maps incosistent (str) source labels to our defined (str) labels    
# target_to_num_metadata =  '/data/scratch/r393017/Datametadata_new/labelMap_no_binning.json'                    #Maps our defined (str) labels to numeric (int) labels

# #Relevant col headers as defined in the metadata file
# file_col = 'File Location'
# label_col = 'Series Description'
# uid_col = 'Series UID'
# ###End user input

# #TODO: add function below to the class, and make this rewrite_label_map a function within that class
# # So we also feed the class both source_to_str_metadata and target_to_num_metadata
# #TODO: remove the import of source_to_str_metadata. Instead, read of our result after mapping source_labels to string_labels. Get #of unique labels, 
# # and create a JSON yourself, containng only the relevant string labels and their corresponding numeric labels

# labelmetadata = rewrite_label_map(Utils.load_json(source_to_str_metadata), 
#                                 Utils.load_json(target_to_num_metadata))



# #Get the relevant metadata: [file_paths,labels]
# source_metadata = metadata.get_relevant_metadata(add_root=True)
# # metadata.set_metadata_columns()       #TODO: I believe this function is redundant / can be removed ?
# metadata.remove_nan()
# metadata.path_spaces_to_underscores()
# metadata.relabel_data()

# #CRASHES HERE
# unique_labels = metadata.get_unique_numeric_labels()  


# print(unique_labels)  
# if not metadata.unmapped_input_labels:
#     metadata.move_test_files(target_dir = 'test_data')
#     metadata.metadata.to_csv('test_data.tsv', index = False, sep='\t', lineterminator='\n', header=False)

# testdata = 'test_data.tsv'
# Utils.get_exisiting_labels(testdata, num_map='labelMap_no_binning.json')

# file_frame = pd.read_csv('FileFrame.csv')
# original_to_new_path = file_frame[['originPath','NIFTI_path', 'sliced']]
# original_to_new_path = file_frame[file_frame['sliced'] == 'True']

# #NOw we merge the dataframes, and only keep the nifty slice paths and their train labels


# #FIXME: I believe that all below can be thrown away ?
# # original_labels = pd.read_csv('test_data_new.tsv', sep='\t')
# # original_labels.columns = ['originPath', 'label']
# # print(original_labels.head())
# # new_labels = pd.read_csv('FileFrame.csv')

# # labels_input = pd.merge(new_labels, original_labels, on='originPath', how='left')
# # print(labels_input.head())
# # ##Remove not sliced files
# # labels_input = labels_input[labels_input['sliced'] == True]
# # print(labels_input.head())

# # labels_input = labels_input[['NIFTI_path', 'label']]
# # print(len(labels_input.index))
# # print(len(original_labels.index))
# # print(len(new_labels.index))
# # labels_input.to_csv('train_labels_prostate.tsv',index = False, sep='\t', lineterminator='\n', header=False)