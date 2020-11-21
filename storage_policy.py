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

    @abstractmethod
    def dump(self, word_to_docs_mapping, filepath: str) -> None:
        pass

    @abstractmethod
    def load(self, filepath: str) -> dict:
        pass


class JsonStoragePolicy(StoragePolicy):
    def dump(self, python_dict: dict, file_path: str):
        with open(file_path, "w") as file:
            json.dump(python_dict, file)

        return None

    def load(self, file_path: str) -> dict:
        with open(file_path, "r") as file:
            result = json.load(file)

        return result


class PklStoragePolicy(StoragePolicy):
    def dump(self, word_to_docs_mapping, filepath: str):
        with open(filepath, "wb") as f:
            pickle.dump(word_to_docs_mapping, f)

        return None

    def load(self, filepath: str) -> dict:
        with open(filepath, "rb") as f:
            result = pickle.load(f)

        return result


class ZlibStoragePolicy(StoragePolicy):
    def __init__(self, encoding, level=6):
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
    def __init__(self, encoding):
        self.encoding = encoding

    def dump(self, word_to_docs_mapping, filepath: str) -> None:
        raise NotImplemented

    def load(self, filepath: str) -> dict:
        raise NotImplemented
#
# transform = {'1':"s", # строка
#              '2':"H" # unsigned short
#              }
#
# mask_for_header_info = "2I"
# a = struct.pack(mask_for_header_info, 101, 32)
#
# a write to file in wb mode
# a read from file in rb mode
#
# b = struct.unpack(mask_for_header_info, a)
# word_mask = str(b[0])
# doc_id_mask = str(b[1])
#
# word_mask = word_mask[:-1] + transform[word_mask[-1]]
# doc_id_mask = doc_id_mask[:-1] + transform[doc_id_mask[-1]]
# mask_for_info = word_mask + " " + doc_id_mask
#
# # TODO нужно как то понять сколько байт информации в 1 сообщении по его маске
# info_raw = прочитать n байт из файла
# struct.unpack(mask_for_info, info_raw)
#
#
# int_to_type = {'1':"s", # строка
#                '2':"H" # unsigned short
#                }
#
# type_to_int = {"s":"1", # строка
#                "H":"2" # unsigned short
#                }
#
# def dump(word_to_docs_mapping, filepath: str, coding="utf-8")->None:
#     meta_info_mask = "2I"
#
#     with open(filepath, "wb") as f:
#         for word in word_to_docs_mapping:
#             # маска для упаковки данных
#             letter_count = len(word)
#             doc_id_count = len(word_to_docs_mapping[word])
#             info_mask = f"{letter_count}s {doc_id_count}H"
#
#             # преобразование метаинформации
#             # "10s 3H" -> [101, 32]
#             meta_info_1 = int(info_mask.split()[0].replace("s",type_to_int["s"]))
#             meta_info_2 = int(info_mask.split()[0].replace("H",type_to_int["H"]))
#
#             # запись метаинформации
#             meta_info = struct.pack(meta_info_mask,meta_info_1, meta_info_2)
#             f.write(meta_info)
#
#             # запись информации
#             info = struct.pack(info_mask,
#                                word.encode(coding),
#                                *word_to_docs_mapping[word])
#             f.write(info)
#
#     return None
#
#
# def load(filepath: str, coding="utf-8") -> dict:
#
#     result = {}
#
#     meta_info_mask = "2I"
#     meta_info_size = struct.calcsize(meta_info_mask)
#
#     with open(filepath, "rb") as f:
#         meta_info_bytes = f.read(meta_info_size)
#         meta_info = struct.unpack(meta_info_mask, meta_info_bytes)
#
#         word_mask = str(meta_info[0])
#         doc_id_mask = str(meta_info[1])
#
#         word_mask = word_mask[:-1] + int_to_type[word_mask[-1]]
#         doc_id_mask = doc_id_mask[:-1] + int_to_type[doc_id_mask[-1]]
#         info_mask = word_mask + " " + doc_id_mask
#
#         info_size = struct.calcsize(info_mask)
#         info_bytes = f.read(info_size)
#         info = struct.unpack(info_mask, info_bytes)
#
#         info_word = info[0].decode(coding)
#         info_docid = list(info[1:])
#
#         result[info_word] = info_docid
#
#     return result