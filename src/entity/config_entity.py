from dataclasses import dataclass
from src.constant import TIMESTAMP
from src.entity.metadata_entity import DataIngestionMetaData
from datetime import datetime
from src.exception import ComplaintException
import os
import sys 

DATA_INGESTION_DIR = 'data_ingestion'
DATA_INGESTION_DOWNLOADED_DATA_DIR = 'downloaded_files'
DATA_INGESTION_FILE_NAME = 'src'
DATA_INGESTION_FEATURE_STORE_DIR = 'feature_store'
DATA_INGESTION_FAILED_DIR = 'failed_downloaded_files'
DATA_INGESTION_METADATA_FILE_NAME = 'meta_info.yaml'
DATA_INGESTION_MIN_START_DATE = '2023-03-01'
DATA_INGESTION_METADATA_FILE_NAME = 'meta_info.yaml'
DATA_INGESTION_DATA_SOURCE_URL = f"https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/" \
                      f"?date_received_max=<todate>&date_received_min=<fromdate>" \
                      f"&field=all&format=json"


@dataclass
## Training Pipeline
class TrainingPipelineConfig:
    pipeline_name:str = "artifact"
    artifact_dir:str = os.path.join(pipeline_name, TIMESTAMP)

## Data Ingestion Config
class DataIngestionConfig:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig,
                        from_date = DATA_INGESTION_MIN_START_DATE,
                        to_date = None):

        try:
            self.from_date = from_date
            min_start_date = datetime.strptime(DATA_INGESTION_MIN_START_DATE,"%Y-%m-%d")
            from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")

            ## if given form_date is less then the derived from_date then the derived from_date would be considered
            if from_date_obj < min_start_date:
                self.from_date = DATA_INGESTION_MIN_START_DATE

            ## if to_date is not given consider the present date
            if to_date is None:
                self.to_date = datetime.now().strftime("%Y-%m-%d")

            ## Declaring the master directory to store all the data ingestion related files
            data_ingestion_master_dir = os.path.join(os.path.dirname(training_pipeline_config.artifact_dir), DATA_INGESTION_DIR)
            self.data_ingestion_dir = os.path.join(data_ingestion_master_dir, TIMESTAMP)
            self.metadata_file_path = os.path.join(data_ingestion_master_dir, DATA_INGESTION_METADATA_FILE_NAME)

            ## Getting the DataIngestion Metadata
            data_ingestion_metadata = DataIngestionMetaData(metadata_file_path=self.metadata_file_path)
            ## If metadata is already present than download the data form dates which are not present   
            if data_ingestion_metadata.is_metadata_file_present:
                metadata_info = data_ingestion_metadata.get_metadata_info()
                self.from_date = metadata_info.to_date

            ## Preparing the download dir
            self.download_dir = os.path.join(self.data_ingestion_dir, DATA_INGESTION_DOWNLOADED_DATA_DIR)
            self.failed_download_dir = os.path.join(self.data_ingestion_dir, DATA_INGESTION_FAILED_DIR)
            self.file_name = DATA_INGESTION_FILE_NAME
            self.feature_store_dir = os.path.join(data_ingestion_master_dir, DATA_INGESTION_FEATURE_STORE_DIR)
            self.data_source_url = DATA_INGESTION_DATA_SOURCE_URL
        
        except Exception as e:
            ComplaintException(e, sys)


        

