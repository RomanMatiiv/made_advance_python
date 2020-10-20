import json

from storage_policy import JsonStoragePolicy


class InvertedIndex:
    def __init__(self):
        self.word_in_docs_map = {}

    def query(self, words: list) -> list:
        """Return the list of relevant documents for the given query"""
        documents = []

        for word in words:
            try:
                documents.extend(self.word_in_docs_map[word])
            except KeyError:
                pass  # если искомое слово отсутствует в индексе

        documents = list(set(documents))

        return documents

    def dump(self, filepath: str, storage_policy=JsonStoragePolicy):
        storage_policy.dump(self.word_in_docs_map, filepath)

    @classmethod
    def load(cls, filepath: str, storage_policy=JsonStoragePolicy):
        inverted_index = cls()

        inverted_index.word_in_docs_map = storage_policy.load(filepath)

        return inverted_index


def load_documents(filepath: str) -> list:
    documents = []

    with open(filepath) as fin:
        for row in fin.readlines():
            doc_id, text = (row.strip()
                               .split('\t'))
            doc_id = int(doc_id)
            documents.append([doc_id, text])

    return documents


def build_inverted_index(documents: list):
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
