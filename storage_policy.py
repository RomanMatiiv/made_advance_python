from abc import ABC
from abc import abstractmethod
import json


class StoragePolicy(ABC):

    @staticmethod
    @abstractmethod
    def dump(word_to_docs_mapping, filepath: str):
        pass

    @staticmethod
    @abstractmethod
    def load(filepath: str):
        pass


class JsonStoragePolicy(StoragePolicy):
    @staticmethod
    def dump(python_dict: dict, file_path: str):
        with open(file_path, "w") as file:
            json.dump(python_dict, file)

        return None

    @staticmethod
    def load(file_path: str) -> dict:
        with open(file_path, "r") as file:
            result = json.load(file)

        return result
