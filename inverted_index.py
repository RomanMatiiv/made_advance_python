"""
Модуль, в котором
1. Реализован класс с инвертированны индексом
2. CLI интерфейс для работы с инвертированным индексом
"""
import sys
from argparse import ArgumentParser
from argparse import ArgumentTypeError
from argparse import FileType
import logging
from io import TextIOWrapper
from storage_policy import JsonStoragePolicy


logger = logging.getLogger(__name__)


class Document:
    """
    Структура данных, представляющее собой документ

    * id - номер документа
    * name - название документа
    * content - содержание документа
    """
    __slots__ = ("id", "name", "content")

    def __init__(self, id: int, name: str, content: str):
        self.id = id
        self.name = name
        self.content = content

    def __eq__(self, other):
        if (self.id == other.id and
            self.name == other.name and
            self.content == other.content):
            return True
        else:
            return False


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
        queries = []

        for word in words:
            try:
                queries.append(self.word_in_docs_map[word])
            except KeyError:
                queries.append([])

        union = set(queries[0])

        for query in queries:
            union = union.intersection(query)

        return union

    def dump(self, filepath: str, storage_policy=JsonStoragePolicy()):
        """
        Сохраняет словарь с инвертированным индексом

        Args:
            filepath: путь до файла в который сохранить
            storage_policy: метод обработки сохраняемого объекта

        Returns: None

        """
        storage_policy.dump(self.word_in_docs_map, filepath)

    @classmethod
    def load(cls, filepath: str, storage_policy=JsonStoragePolicy()):
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


class EncodedFileType(FileType):
    def __call__(self, string):
        # the special argument "-" means sys.std{in,out}
        if string == '-':
            if 'r' in self._mode:
                stdin = TextIOWrapper(sys.stdin.buffer, self._encoding)
                return stdin
            elif 'w' in self._mode:
                stdout = TextIOWrapper(sys.stdout.buffer, self._encoding)
                return stdout
            else:
                msg = f'argument "-" with mode {self._mode}'
                raise ValueError(msg)
        # all other arguments are used as file names
        try:
            return open(string, self._mode, self._bufsize, self._encoding,
                        self._errors)
        except OSError as e:
            message = f"can't open {string}: {e}"
            raise ArgumentTypeError(message)


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
             [Document(doc_id, doc_name, doc_content)
              ...,
              Document(doc_id, doc_name, doc_content)
             ]
    """
    documents = []

    with open(filepath) as fin:
        for row in fin.readlines():
            doc_id, text = (row.strip()
                               .split('\t', maxsplit=1))

            doc_id = int(doc_id)
            doc_name = text.split()[0]
            doc_content = text.replace(doc_name, "")
            doc_content = doc_content.strip()

            doc = Document(doc_id, doc_name, doc_content)

            documents.append(doc)

    return documents


def build_inverted_index(documents: list):
    """
    Построение ивертированного индекса

    По списку с документами строит ивертированный индекс

    Args:
        documents: массив с документами
                   в формате
                   [
                   Document(id,name,context),
                    ...
                   Document(id,name,context)
                   ]

    Returns: InvertedIndex

    """
    inverted_index = InvertedIndex()

    for doc in documents:
        content = doc.name + " " + doc.content
        for word in content.split():
            if word in inverted_index.word_in_docs_map.keys():
                inverted_index.word_in_docs_map[word].append(doc.id)
            else:
                inverted_index.word_in_docs_map[word] = [doc.id]

    # Удаление дубликатов
    for key in inverted_index.word_in_docs_map.keys():
        inverted_index.word_in_docs_map[key] = list(set(inverted_index.word_in_docs_map[key]))

    return inverted_index


# def main():
#     documents = load_documents("/path/to/dataset")
#     inverted_index = build_inverted_index(documents)
#     inverted_index.dump("/path/to/inverted.index")
#     inverted_index = InvertedIndex.load("/path/to/inverted.index")
#     document_ids = inverted_index.query(["two", "words"])

def build_callback(arguments):
    documents = load_documents(arguments.dataset)
    inverted_index = build_inverted_index(documents)
    inverted_index.dump(arguments.output)


def query_callback(arguments):
    logger.debug(arguments)

    if arguments.query_from_stdin:
        queries = arguments.query_from_stdin
        logger.debug(queries)
    else:
        queries = []
        for query in arguments.query_from_file:
            query = query.strip()
            queries.append(query)
        logger.debug(queries)

    inverted_index = InvertedIndex.load(arguments.index)
    document_ids = inverted_index.query(queries)

    document_ids_str = [str(i) for i in document_ids]
    result = ",".join(document_ids_str)
    print(result)


def parse_arguments():
    args_parser = ArgumentParser()
    subparsers = args_parser.add_subparsers()

    # BUILD
    build_description = """
                        create inverted index from dataset.
                        Format dataset doc_id<tab>doc_text<unix line separator>
                        """
    build = subparsers.add_parser(name="build",
                                  description=build_description)
    build.set_defaults(callback=build_callback)

    build.add_argument("--dataset",
                       dest="dataset",
                       help="path to file with documents",
                       required=True,
                       type=str)
    build.add_argument("--output",
                       dest="output",
                       help="path to file with saved inverted index",
                       required=True,
                       type=str)

    # QUERY
    query_description = """
                        Show which document contains query
                        If query is more than one worlds algo will file
                        documents union.
                        """
    query = subparsers.add_parser(name="query",
                                  description=query_description)
    query.set_defaults(callback=query_callback)
    query.add_argument("--index",
                       dest="index",
                       help="path to file with saved inverted index",
                       required=True,
                       type=str)

    query_group = query.add_mutually_exclusive_group()
    query_group.add_argument("--query",
                             dest="query_from_stdin",
                             help=None,
                             required=False,
                             nargs="+"
                             )
    query_group.add_argument("--query-file-utf8",
                             dest="query_from_file",
                             help="file with query in utf8 coding",
                             type=EncodedFileType('r', encoding="utf8"),
                             default=TextIOWrapper(sys.stdin.buffer)
                             )
    query_group.add_argument("--query-file-cp1251",
                             dest="query_from_file",
                             help="file with query in cp1251 coding",
                             type=EncodedFileType('r', encoding="cp1251"),
                             default=TextIOWrapper(sys.stdin.buffer)
                             )

    args = args_parser.parse_args()

    return args


def setup_logger():
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    return None


if __name__ == "__main__":
    setup_logger()
    args = parse_arguments()

    args.callback(args)
