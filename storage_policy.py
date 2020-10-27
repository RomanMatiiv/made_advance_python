"""
Модуль в котором определены политики(способы)
сохранение словаря с инвертированным индексом на жесткий диск
"""
import json
import pickle
import zlib
from abc import ABC
from abc import abstractmethod


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


class ZlibStoragePolicy(StoragePolicy):
    @staticmethod
    def dump(word_to_docs_mapping, filepath: str, **kwargs) -> None:
        try:
            level = kwargs["level"]
        except KeyError:
            level = 6

        w2d_mapping_str = json.dumps(word_to_docs_mapping)
        w2d_mapping_bytes = bytes(w2d_mapping_str, encoding="utf8")
        w2d_mapping_compress = zlib.compress(w2d_mapping_bytes, level=level)
        with open(filepath, "wb") as f:
            f.write(w2d_mapping_compress)

        return None

    @staticmethod
    def load(filepath: str) -> dict:
        with open(filepath, "rb") as f:
            data = f.read()
            data_decompress = zlib.decompress(data)
            data_str = data_decompress.decode(encoding="utf8")
            data_dict = json.loads(data_str)

        return data_dict