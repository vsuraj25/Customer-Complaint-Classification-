from src.config import mongo_client
from src.entity.artifact_entity import ModelEvaluationArtifact

class ModelEvaluationArtifactData:
    ## Initializing mongodb client and related variables
    def __init__(self):
        
        self.client = mongo_client
        self.database_name = "finance_complaint_artifact"
        self.collection_name = "evaluation"
        self.collection = self.client[self.database_name][self.collection_name]

    ## Saving the evaluation artifact in the database
    def save_eval_artifact(self, model_eval_artifact : ModelEvaluationArtifact):
        self.collection.insert_one(model_eval_artifact.to_dict())

    ## Fetching the evaluation artifact from the database
    def get_eval_artifact(self,query):
        self.collection.find_one(query)