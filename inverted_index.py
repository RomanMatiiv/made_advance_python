"""
Модуль, в котором
1. Реализован класс с инвертированны индексом
2. CLI интерфейс для работы с инвертированным индексом
"""
from storage_policy import JsonStoragePolicy


class InvertedIndex:
    """
    Класс в котором реализован инвертированный индекс

    Инвертированный индекс представляет собой словарь, где ключами являются
    слова (термы), а значениями - списки идентификаторов документов,
    в которых указанный терм встречается
    """
    def __init__(self):
        self.word_in_docs_map = {}

    def query(self, words: list) -> list:
        """
        Возвращает список документов, в которых данные слова встречаются

        Если слов на вход 2 и более, то вернет только общие документы
        Те документы в которых есть 1-е слово И 2-е слово

        Args:
            words: список со словами

        Returns: список с документами

        """
        documents = []

        for word in words:
            try:
                documents.extend(self.word_in_docs_map[word])
            except KeyError:
                pass  # если искомое слово отсутствует в индексе

        documents = list(set(documents))

        return documents

    def dump(self, filepath: str, storage_policy=JsonStoragePolicy):
        """
        Сохраняет словарь с инвертированным индексом

        Args:
            filepath: путь до файла в который сохранить
            storage_policy: метод обработки сохраняемого объекта

        Returns: None

        """
        storage_policy.dump(self.word_in_docs_map, filepath)

    @classmethod
    def load(cls, filepath: str, storage_policy=JsonStoragePolicy):
        """
        Загружает объект инвертированнного индекса с диска

        По факту загружает только словарь для объекта,
        а потом уже и сам объект воссоздает

        Args:
            filepath: путь до файла в который сохранить
            storage_policy: метод обработки сохраняемого объекта

        Returns: InvertedIndex
        """
        inverted_index = cls()

        inverted_index.word_in_docs_map = storage_policy.load(filepath)

        return inverted_index


def load_documents(filepath: str) -> list:
    """
    Загружает документы с диска

    Документы должны быть в формате

    doc_id<знак табуляции>doc_text<перенос_строки>

    Например
    1/tHello world/n
    2/tHow are you?/n

    Args:
        filepath: пусть до файла с документами

    Returns: массив с документами
             в формате
             [
             [doc_id, doc_text],
             ...
             ]
    """
    documents = []

    with open(filepath) as fin:
        for row in fin.readlines():
            doc_id, text = (row.strip()
                               .split('\t'))
            doc_id = int(doc_id)
            documents.append([doc_id, text])

    return documents


def build_inverted_index(documents: list):
    """
    Построение ивертированного индекса

    По списку с документами строит ивертированный индекс

    Args:
        documents: массив с документами
                   в формате
                   [
                    [doc_id, doc_text],
                    ...
                   ]

    Returns: InvertedIndex

    """
    inverted_index = InvertedIndex()

    for doc_id, doc in documents:
        for word in doc.split():
            if word in inverted_index.word_in_docs_map.keys():
                inverted_index.word_in_docs_map[word].append(doc_id)
            else:
                inverted_index.word_in_docs_map[word] = [doc_id]

    # Удаление дубликатов
    for key in inverted_index.word_in_docs_map.keys():
        inverted_index.word_in_docs_map[key] = list(set(inverted_index.word_in_docs_map[key]))

    return inverted_index


def main():
    documents = load_documents("/path/to/dataset")
    inverted_index = build_inverted_index(documents)
    inverted_index.dump("/path/to/inverted.index")
    inverted_index = InvertedIndex.load("/path/to/inverted.index")
    document_ids = inverted_index.query(["two", "words"])


if __name__ == "__main__":
    main()
