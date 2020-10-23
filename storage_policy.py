from abc import ABC
from abc import abstractmethod
import json
import pickle


class StoragePolicy(ABC):

    @staticmethod
    @abstractmethod
    def dump(word_to_docs_mapping, filepath: str) -> None:
        pass

    @staticmethod
    @abstractmethod
    def load(filepath: str) -> dict:
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


class PklStoragePolicy(StoragePolicy):
    @staticmethod
    def dump(word_to_docs_mapping, filepath: str):
        with open(filepath, "wb") as f:
            pickle.dump(word_to_docs_mapping, f)

        return None

    @staticmethod
    def load(filepath: str) -> dict:
        with open(filepath, "rb") as f:
            result = pickle.load(f)

        return result
