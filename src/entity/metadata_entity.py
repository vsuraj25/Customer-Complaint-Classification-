from src.exception import ComplaintException
from src.logger import logging
from src.utils import write_yaml_file, read_yaml_file
import os
import sys

from collections import namedtuple

DataIngestionMetaDataInfo = namedtuple("DataIngestionMetaDataInfo", ["from_date", "to_date", "data_file_path"])

class DataIngestionMetaData:

    def __init__(self, metadata_file_path):
        self.metadata_file_path = metadata_file_path

    @property
    ## Check if Data Ingestion Metadata is available or not
    def is_metadata_file_present(self)-> bool:
        return os.path.exists(self.metadata_file_path)
    
    ## If Metadata available write it as a yaml file
    def write_metadata_info(self, from_date:str, to_date:str, data_file_path: str):
        try:
            metadata_info = DataIngestionMetaDataInfo(
                from_date= from_date,
                to_date= to_date,
                data_file_path= data_file_path
            )
            write_yaml_file(file_path=self.metadata_file_path, data = metadata_info._asdict())

        except Exception as e:
            raise ComplaintException(e, sys)
        
    ## Return the metadata info
    def get_metadata_info(self) -> DataIngestionMetaDataInfo:
        try:
            if not self.is_metadata_file_present():
                raise Exception("No Metadata file available")
            metadata = read_yaml_file(self.metadata_file_path)
            metadata_info = DataIngestionMetaDataInfo(**(metadata))
            logging.info(metadata)
            return metadata_info

        except Exception as e:
            raise ComplaintException(e, sys)