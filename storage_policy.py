import json


class StoragePolicy:
    @staticmethod
    def dump(word_to_docs_mapping, filepath: str):
        pass

    @staticmethod
    def load(filepath: str):
        pass


class JsonStoragePolicy:
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
