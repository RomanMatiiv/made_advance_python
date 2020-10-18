import json


class InvertedIndex:
    def __init__(self):
        self.word_in_docs = {}

    def query(self, words: list) -> list:
        """Return the list of relevant documents for the given query"""
        documents = []

        for word in words:
            try:
                documents.extend(self.word_in_docs[word])
            except KeyError:
                pass # если искомое слово отсутствует в индексе

        documents = list(set(documents))

        return documents


    def dump(self, filepath: str):
        with open(filepath, "w") as file:
            json.dump(self.word_in_docs, file)

    @classmethod
    def load(cls, filepath: str):
        inverted_index = cls()

        with open(filepath, "r") as file:
            inverted_index.word_in_docs = json.load(file)

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
            if word in inverted_index.word_in_docs.keys():
                inverted_index.word_in_docs[word].append(doc_id)
            else:
                inverted_index.word_in_docs[word] = [doc_id]

    # Удаление дубликатов
    for key in inverted_index.word_in_docs.keys():
        inverted_index.word_in_docs[key] = list(set(inverted_index.word_in_docs[key]))

    return inverted_index


def main():
    documents = load_documents("/path/to/dataset")
    inverted_index = build_inverted_index(documents)
    inverted_index.dump("/path/to/inverted.index")
    inverted_index = InvertedIndex.load("/path/to/inverted.index")
    document_ids = inverted_index.query(["two", "words"])


if __name__ == "__main__":
    main()

