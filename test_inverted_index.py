import json
import os

import pytest

import inverted_index as IIS
from storage_policy import JsonStoragePolicy


def test_load_documents_on_doesnt_exist_file():
    path_to_doesnt_exist_file = "fake/path/to/dataset.txt"

    with pytest.raises(FileNotFoundError):
        IIS.load_documents(path_to_doesnt_exist_file)


def test_load_documents_on_2_doc():
    docs = "1\tHello\n2\tworld\n"

    expected_docs = [[1, "Hello"],
                     [2, "world"]]

    with File(content=docs) as f:
        load_docs = IIS.load_documents(f.file_path)

        assert expected_docs == load_docs


def test_load_documents_on_incorrect_doc_struct():
    docs = "1\tHello\n2\tworld\tworld\n"

    with pytest.raises(ValueError):
        with File(content=docs) as f:
            IIS.load_documents(f.file_path)


def test_build_inverted_index():

    expect_inverted_index = {"hello": [1, 2],
                             "world": [2]}

    docs = [[1, "hello"],
            [2, "hello  world"]]
    inverted_index = IIS.build_inverted_index(docs)

    assert expect_inverted_index == inverted_index.word_in_docs_map


def test_dump_inverted_index():
    inverted_index = IIS.InvertedIndex()

    inverted_index.word_in_docs_map = {"hello": [1, 2],
                                   "world": [2]}

    inverted_index.dump("tmp.json")

    os.remove("tmp.json")


def test_dump_and_load_inverted_index():

    expected ={"hello": [1, 2],
               "world": [2]}

    inverted_index = IIS.InvertedIndex()

    inverted_index.word_in_docs_map = expected

    inverted_index.dump("tmp.json")
    del inverted_index

    inverted_index = IIS.InvertedIndex.load("tmp.json")

    os.remove("tmp.json")

    assert expected == inverted_index.word_in_docs_map


def test_query_in_inverted_index():
    inverted_index = IIS.InvertedIndex()
    inverted_index.word_in_docs_map = {"word": [1, 4],
                                   "hello": [1, 2],
                                   "covid": [3, 4]}

    expect_docs = [1, 2, 4]
    docs = inverted_index.query(["hello", "word"])

    assert len(expect_docs) == len(docs)

    # проверка на то, что все документы найдены
    for doc in docs:
        assert doc in expect_docs


def test_query_not_in_inverted_index():
    inverted_index = IIS.InvertedIndex()
    inverted_index.word_in_docs_map = {"word": [1, 4],
                                   "hello": [1, 2],
                                   "covid": [3, 4]}

    expect_docs = []
    docs = inverted_index.query(["ABS", "DDDDD"])

    assert len(expect_docs) == len(docs)


def test_json_storage_policy_dump(tmpdir):
    file = tmpdir.join("tmp_file")
    mapping = {"d": [1, 2]}

    JsonStoragePolicy.dump(mapping, file_path=file.strpath)


def test_json_storage_policy_load(tmpdir):
    file = tmpdir.join("tmp.json")

    expect_mapping = {"a": [3, 6],
                      "rr": [1]}

    with open(file.strpath, "w") as f_out:
        json.dump(expect_mapping, f_out)

    mapping = JsonStoragePolicy.load(file.strpath)

    assert expect_mapping == mapping


class File:
    def __init__(self, content, file_path="file_for_test.txt"):
        self.file_path = file_path
        self.content = content

        self._write_to_file(content)

    def _write_to_file(self, content):
        with open(self.file_path, "w") as fout:
            fout.write(content)

    def _remove_file(self):
        os.remove(self.file_path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._remove_file()