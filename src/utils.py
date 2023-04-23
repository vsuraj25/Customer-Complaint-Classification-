from .exception import ComplaintException
import yaml
import os
import sys

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