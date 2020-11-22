"""
Модуль в котором определены политики(способы)
сохранение словаря с инвертированным индексом на жесткий диск
"""
import json
import pickle
import zlib
from abc import ABC
from abc import abstractmethod
import struct


class StoragePolicy(ABC):
    """
    Интерфейс для сохранения питоновского словаря на диск
    """
    @abstractmethod
    def dump(self, word_to_docs_mapping, filepath: str) -> None:
        pass

    @abstractmethod
    def load(self, filepath: str) -> dict:
        pass


class JsonStoragePolicy(StoragePolicy):
    """
    Сохранение питоновского словаря на диск
    """
    def dump(self, python_dict: dict, file_path: str):
        with open(file_path, "w") as file:
            json.dump(python_dict, file)

        return None

    def load(self, file_path: str) -> dict:
        with open(file_path, "r") as file:
            result = json.load(file)

        return result


class PklStoragePolicy(StoragePolicy):
    """
    Сохранение питоновского словаря на диск
    """
    def dump(self, word_to_docs_mapping, filepath: str):
        with open(filepath, "wb") as f:
            pickle.dump(word_to_docs_mapping, f)

        return None

    def load(self, filepath: str) -> dict:
        with open(filepath, "rb") as f:
            result = pickle.load(f)

        return result


class ZlibStoragePolicy(StoragePolicy):
    """
    Сохранение питоновского словаря с помощью библиотеки zlib
    """
    def __init__(self, encoding, level=6):
        """

        Args:
            encoding: кодировка в которой находятся данные
            level: степень сжатия (от 0 до 9)
        """
        self.encoding = encoding
        self.level = level

    def dump(self, word_to_docs_mapping, filepath: str, **kwargs) -> None:

        w2d_mapping_str = json.dumps(word_to_docs_mapping)
        w2d_mapping_bytes = bytes(w2d_mapping_str, encoding=self.encoding)
        w2d_mapping_compress = zlib.compress(w2d_mapping_bytes, level=self.level)
        with open(filepath, "wb") as f:
            f.write(w2d_mapping_compress)

        return None

    def load(self, filepath: str) -> dict:
        with open(filepath, "rb") as f:
            data = f.read()
            data_decompress = zlib.decompress(data)
            data_str = data_decompress.decode(encoding=self.encoding)
            data_dict = json.loads(data_str)

        return data_dict


class StructStoragePolicy(StoragePolicy):
    """
    Сохранение питоновского словаря на диск
    """
    def __init__(self, encoding):
        """

        Args:
            encoding: кодировка в которой находятся данные
        """
        self.encoding = encoding

        self.int_to_type = {
                            '1': "s",  # строка
                            '2': "H"  # unsigned short
                           }
        self.type_to_int = {
                            "s": "1",  # строка
                            "H": "2"  # unsigned short
                            }
        self.meta_info_mask = "2I"

    def dump(self, word_to_docs_mapping, filepath: str) -> None:
        with open(filepath, "wb") as f:
            for word in word_to_docs_mapping:
                # маска для упаковки данных
                letter_count = len(word.encode(self.encoding))
                doc_id_count = len(word_to_docs_mapping[word])
                info_mask = f"{letter_count}s {doc_id_count}H"

                # преобразование метаинформации
                # "10s 3H" -> [101, 32]
                meta_info_1 = int(info_mask.split()[0].replace("s", self.type_to_int["s"]))
                meta_info_2 = int(info_mask.split()[1].replace("H", self.type_to_int["H"]))

                # запись метаинформации
                meta_info = struct.pack(self.meta_info_mask, meta_info_1, meta_info_2)
                f.write(meta_info)

                # запись информации
                info = struct.pack(info_mask,
                                   word.encode(self.encoding),
                                   *word_to_docs_mapping[word])
                f.write(info)

        return None

    def load(self, filepath: str) -> dict:
        result = {}

        meta_info_size = struct.calcsize(self.meta_info_mask)

        with open(filepath, "rb") as f:
            meta_info_bytes = f.read(meta_info_size)

            while meta_info_bytes:
                meta_info = struct.unpack(self.meta_info_mask, meta_info_bytes)

                word_mask = str(meta_info[0])
                doc_id_mask = str(meta_info[1])

                word_mask = word_mask[:-1] + self.int_to_type[word_mask[-1]]
                doc_id_mask = doc_id_mask[:-1] + self.int_to_type[doc_id_mask[-1]]
                info_mask = word_mask + " " + doc_id_mask

                info_size = struct.calcsize(info_mask)
                info_bytes = f.read(info_size)
                info = struct.unpack(info_mask, info_bytes)

                info_word = info[0].decode(self.encoding)
                info_docid = list(info[1:])

                result[info_word] = info_docid

                meta_info_bytes = f.read(meta_info_size)

        return result
