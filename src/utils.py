from .exception import ComplaintException
from .logger import logging
import yaml
import os
import sys
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import DataFrame

def write_yaml_file(file_path: str, data : dict = None):
    """
    Writes a dictionary into a yaml file
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok = True)
        with open(file_path, 'w') as yaml_file:
            if data is not None:
                yaml.dump(data, yaml_file)

    except Exception as e:
        raise ComplaintException(e, sys)
    

def read_yaml_file(file_path: str):
    """
    Reads the yaml file and returns it as a dictionary
    """
    try:
        with open(file_path, 'rb') as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise ComplaintException(e, sys)

def get_score(dataframe: DataFrame, metric_name, label_col, prediction_col) -> float:
    try:
        evaluator = MulticlassClassificationEvaluator(
            labelCol=label_col, predictionCol=prediction_col,
            metricName=metric_name)
        score = evaluator.evaluate(dataframe)
        print(f"{metric_name} score: {score}")
        logging.info(f"{metric_name} score: {score}")
        return score
    except Exception as e:
        raise ComplaintException(e, sys)