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
    Интерфейс для сохранения и извлечение инвертированного индекса на диск
    """
    @abstractmethod
    def dump(self, word_to_docs_mapping, filepath: str) -> None:
        pass

    @abstractmethod
    def load(self, filepath: str) -> dict:
        pass


class JsonStoragePolicy(StoragePolicy):
    """
    Сохранение и извлечение инвертированного индекса на диск
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
    Сохранение и извлечение инвертированного индекса на диск
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
    Сохранение и извлечение инвертированного индекса на диск
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
    Сохранение и извлечение инвертированного индекса на диск
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
        """
        Сохранение инвертированного индекса на жесткий диск

        Args:
            word_to_docs_mapping: инвертированный индекс
            filepath: путь до файла куда сохранять

        Returns: None

        """
        with open(filepath, "wb") as f:
            for word in word_to_docs_mapping:

                info_mask = self._get_info_mask(word, word_to_docs_mapping[word])

                meta_info = self._coding_mask(info_mask)

                # запись метаинформации
                raw_meta_info = struct.pack(self.meta_info_mask,
                                        meta_info[0],
                                        meta_info[1])
                f.write(raw_meta_info)

                # запись информации
                raw_info = struct.pack(info_mask,
                                       word.encode(self.encoding),
                                       *word_to_docs_mapping[word])
                f.write(raw_info)

        return None

    def _coding_mask(self, info_mask: str) -> list:
        """
        Преобразование маски для последующего ее сохранения

        Делается это для того, чтобы данные обратно распоковать
        нужна будет таже самая маска, что и использоваолась при их упаковке
        но тк для данных маски будут разные то их все нужно будет хранить

        Но но весли перевести все маски в числовой формат, то саму маску уже
        можно закодировать 2 числами
        (те получается что маска для маски универсальна)
        Подробнее см пример
        Args:
            info_mask: маска для сохранеия информации модулем struct

        Returns: массив из 2 чисел

        Пример: "10s 3H" -> [101, 32]
        """

        word_mask = info_mask.split()[0]
        word_mask_struct_type = word_mask[-1]
        word_mask = word_mask.replace(word_mask_struct_type,
                                      self.type_to_int[word_mask_struct_type])
        word_mask = int(word_mask)

        doc_id_mask = info_mask.split()[1]
        doc_id_mask_struct_type = doc_id_mask[-1]
        doc_id_mask = doc_id_mask.replace(doc_id_mask_struct_type,
                                          self.type_to_int[doc_id_mask_struct_type])
        doc_id_mask = int(doc_id_mask)

        return [word_mask, doc_id_mask]

    def _get_info_mask(self, word: str, documents: list):
        """
        Получение маски для упаковки данных модулем struct

        Args:
            word: слово, которое хотим сохранить
            documents: документы, в которых это слово встречается

        Returns: маску, с помощью которой пару (word, documents)
        можно будет сохранить в бинарном формате модулем struct
        """
        letter_count = len(word.encode(self.encoding))
        doc_id_count = len(documents)
        info_mask = f"{letter_count}s {doc_id_count}H"

        return info_mask

    def load(self, filepath: str) -> dict:
        """
        Извлечение инвертированного индекса с жесткого диска

        Args:
            filepath: путь до сохраненного инвертированного индекса

        Returns: инвертированный индекс

        """
        result = {}

        meta_info_size = struct.calcsize(self.meta_info_mask)

        with open(filepath, "rb") as f:
            meta_info_bytes = f.read(meta_info_size)

            while meta_info_bytes:
                meta_info = struct.unpack(self.meta_info_mask, meta_info_bytes)

                info_mask = self._decoding_mask(meta_info)

                info_size = struct.calcsize(info_mask)
                info_bytes = f.read(info_size)
                info = struct.unpack(info_mask, info_bytes)

                word = info[0].decode(self.encoding)
                documents = list(info[1:])

                result[word] = documents

                meta_info_bytes = f.read(meta_info_size)

        return result

    def _decoding_mask(self, meta_info: list) -> str:
        """
        Раскодирование маски

        Которая нужно чтоб данные извлечь и раскодировать

        Тк помимо данных нужно было и маску хранить
        то маска тоже кодировалась числами и уже числа записывались
        Здесь идет преобразование из чисел в маску
        Args:
            meta_info: маска закодированном формате

        Returns: маска для извлечения и раскодировки данныъ

        Пример: [101, 52] -> "10s 5H"
        """

        # 101 -> "10s"
        word_mask = str(meta_info[0])
        word_mask_struct_type = self.int_to_type[word_mask[-1]]
        word_mask = word_mask[:-1] + word_mask_struct_type

        # 52 -> "5H"
        doc_id_mask = str(meta_info[1])
        doc_id_mask_struct_type = self.int_to_type[doc_id_mask[-1]]
        doc_id_mask = doc_id_mask[:-1] + doc_id_mask_struct_type

        info_mask = word_mask + " " + doc_id_mask
        return info_mask
