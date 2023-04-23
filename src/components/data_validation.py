import os, sys
from collections import namedtuple
from typing import List, Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from src.config.spark_manager import spark_session
from src.entity.artifact_entity import DataIngestionArtifact
from src.entity.artifact_entity import DataValidationArtifact
from src.entity.config_entity import DataValidationConfig
from src.entity.schema import FinanceDataSchema
from src.exception import ComplaintException
from src.logger import logging

ERROR_MESSAGE = 'error_msg'
## Creating the report of null data so that if the precentage is greater then a given thresholf,
## the respective column can be dropped
MissingReport = namedtuple("MissingReport", ["total_row", "missing_row", "missing_percentage"])

class DataValidation():
    
    def __init__(self, 
                data_validation_config : DataValidationConfig,
                data_ingestion_artifact : DataIngestionArtifact,
                schema = FinanceDataSchema()
                ):
                try:
                    self.data_validation_config = data_validation_config
                    self.data_ingestion_artifact : DataIngestionArtifact = data_ingestion_artifact
                    self.schema = schema

                except Exception as e:
                    logging.error(e)
                    raise ComplaintException(e,sys)


    def read_data(self) -> DataFrame:
        try:
            dataframe : DataFrame = spark_session.read.parquet(
                self.data_ingestion_artifact.feature_store_file_path
            ).limit(1000)
            logging.info(f"Data frame is created using file: {self.data_ingestion_artifact.feature_store_file_path}")
            logging.info(f"Number of row: {dataframe.count()} and column: {len(dataframe.columns)}")


        except Exception as e:
            logging.error(e)
            raise ComplaintException(e,sys)

    ## Getting the null values report
    @staticmethod
    def get_missing_report(dataframe: DataFrame, ) -> Dict[str, MissingReport]:
        try:
            missing_report: Dict[str:MissingReport] = dict()
            logging.info(f"Preparing missing reports for each column")
            number_of_row = dataframe.count()

            for column in dataframe.columns:
                missing_row = dataframe.filter(f"{column} is null").count()
                missing_percentage = (missing_row * 100) / number_of_row
                missing_report[column] = MissingReport(total_row=number_of_row,
                                                       missing_row=missing_row,
                                                       missing_percentage=missing_percentage
                                                       )
            logging.info(f"Missing report prepared: {missing_report}")
            return missing_report

        except Exception as e:
            raise ComplaintException(e, sys)

    ## Generating a list of unwanted columns and columns with high percentage of null values
    def get_unwanted_and_high_missing_value_columns(self, dataframe: DataFrame, threshold: float = 0.2) -> List[str]:
        try:
            ### Getting the missing report
            missing_report: Dict[str, MissingReport] = self.get_missing_report(dataframe=dataframe)

            ### Listing the unwanted columns
            unwanted_column: List[str] = self.schema.unwanted_columns
            for column in missing_report:
                if missing_report[column].missing_percentage > (threshold * 100):
                    unwanted_column.append(column)
                    logging.info(f"Missing report {column}: [{missing_report[column]}]")
            unwanted_column = list(set(unwanted_column))
            return unwanted_column

        except Exception as e:
            raise ComplaintException(e, sys)

    ## Dropping the unwanted columns
    def drop_unwanted_columns(self, dataframe: DataFrame) -> DataFrame:
        try:
            unwanted_columns: List = self.get_unwanted_and_high_missing_value_columns(dataframe=dataframe, )
            logging.info(f"Dropping feature: {','.join(unwanted_columns)}")
            unwanted_dataframe: DataFrame = dataframe.select(unwanted_columns)

            unwanted_dataframe = unwanted_dataframe.withColumn(ERROR_MESSAGE, lit("Contains many missing values"))

            rejected_dir = os.path.join(self.data_validation_config.rejected_data_dir, "missing_data")
            os.makedirs(rejected_dir, exist_ok=True)
            file_path = os.path.join(rejected_dir, self.data_validation_config.file_name)

            logging.info(f"Writing dropped column into file: [{file_path}]")
            unwanted_dataframe.write.mode("append").parquet(file_path)
            dataframe: DataFrame = dataframe.drop(*unwanted_columns)
            logging.info(f"Remaining number of columns: [{dataframe.columns}]")
            return dataframe
        except Exception as e:
            raise ComplaintException(e, sys)

    ## validating required columns from all the columns
    def is_required_columns_exist(self, dataframe: DataFrame):
        try:
            columns = list(filter(lambda x: x in self.schema.required_columns,
                                  dataframe.columns))

            if len(columns) != len(self.schema.required_columns):
                raise Exception(f"Required column missing\n\
                 Expected columns: {self.schema.required_columns}\n\
                 Found columns: {columns}\
                 ")

        except Exception as e:
            raise ComplaintException(e, sys)

    ## Initaiting the data validation
    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            logging.info(f"Initiating data preprocessing.")
            dataframe: DataFrame = self.read_data()

            logging.info(f"Dropping unwanted columns")
            dataframe: DataFrame = self.drop_unwanted_columns(dataframe=dataframe)

            # validation to ensure that all require column available
            self.is_required_columns_exist(dataframe=dataframe)
            logging.info("Saving preprocessed data.")
            print(f"Row: [{dataframe.count()}] Column: [{len(dataframe.columns)}]")
            print(f"Expected Column: {self.schema.required_columns}\nPresent Columns: {dataframe.columns}")
            os.makedirs(self.data_validation_config.accepted_data_dir, exist_ok=True)
            accepted_file_path = os.path.join(self.data_validation_config.accepted_data_dir,
                                              self.data_validation_config.file_name
                                              )
            dataframe.write.parquet(accepted_file_path)
            artifact = DataValidationArtifact(accepted_file_path=accepted_file_path,
                                              rejected_dir=self.data_validation_config.rejected_data_dir
                                              )
            logging.info(f"Data validation artifact: [{artifact}]")
            return artifact
        except Exception as e:
            raise ComplaintException(e, sys)

    

