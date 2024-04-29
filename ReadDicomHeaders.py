import pydicom
from pydicom.datadict import  dictionary_description
import pandas as pd

def read_all_dicom_headers(dicom_file_path: str) -> list:
    """
    Reads and returns all the headers (tags, descriptions, and values) from a DICOM file.

    Args:
        dicom_file_path (str): Path to the DICOM file.

    Returns:
        list of dict: A list containing dictionaries for each header with 'tag', 'description', 'VR', and 'value'.
    """
    try:
        # Load the DICOM file
        dicom_data = pydicom.dcmread(dicom_file_path, stop_before_pixels=True)

        headers = []

        # Iterate through all data elements in the DICOM file
        for data_element in dicom_data:
            tag = data_element.tag
            description = dictionary_description(tag) if tag in pydicom.datadict.DicomDictionary else "Unknown"
            VR = data_element.VR if hasattr(data_element, 'VR') else "Unknown"
            value = data_element.value

            # Append the header info to the list
            headers.append({
                'description': description,
                'VR': VR,
                'value': str(value)  # converting value to string to avoid issues with complex data types
            })

        return headers

    except Exception as e:
        print(f"Error reading DICOM file: {e}")
        return []

# Example usage


def read_scan_type(dicom_file_path: str) -> str:
    """
    Attempts to determine the MRI scan type from a DICOM file based on series and sequence information.

    Args:
        dicom_file_path (str): Path to the DICOM file.

    Returns:
        str: A string describing the scan type if identified, otherwise 'Unknown'.
    """
    try:
        # Load the DICOM file
        dicom_data = pydicom.dcmread(dicom_file_path)

        # Potential DICOM tags that might contain scan type information
        tags_to_check = {
            'StudyDescription': (0x0008, 0x1030),
            'SeriesDescription': (0x0008, 0x103E),
            'SequenceName': (0x0018, 0x0024),
            'PulseSequenceName': (0x0018, 0x9004),
            'SequenceVariant': (0x0018, 0x0021)
        }

        # Check each tag and return the value if it exists
        for name, tag in tags_to_check.items():
            if tag in dicom_data:
                value = dicom_data[tag].value
                if value:
                    return f"{name} - {value}"

        # If no relevant information is found
        return "Unknown"
    except Exception as e:
        return f"Error reading DICOM file: {e}"

# Example usage
path = '/home/donpi-boxer/BME/Thesis/data/kidney/manifest-xxn3N2Qq630907925598003437/TCGA-KIRC/TCGA-B0-4698/11-22-1985-NA-CT ABDOMEN-45292/1.000000-SCOUT APLAT-41044/1-1.dcm'
scan_type = read_scan_type(path)
#print(scan_type)
all_headers = read_all_dicom_headers(path)
print(all_headers)
header_df = pd.DataFrame(all_headers)
header_df.to_csv('header_info.csv', index=False, header=True)