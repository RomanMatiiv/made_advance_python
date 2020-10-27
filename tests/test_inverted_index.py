import os

import pytest

import inverted_index as IIS


@pytest.fixture()
def small_dataset(tmpdir) -> dict:
    expect = {}

    expect["raw_docs"] = "1\tHello\n2\tworld\n3\thow are you?"

    expect["list_docs"] = [[1, "Hello"],
                           [2, "world"],
                           [3, "how are you?"]]

    file = tmpdir.join("raw_file_on_disc")
    file.write(expect["raw_docs"])
    expect["file_with_raw_docs"] = file.strpath

    return expect


def test_load_documents_on_doesnt_exist_file():
    path_to_doesnt_exist_file = "fake/path/to/dataset.txt"

    with pytest.raises(FileNotFoundError):
        IIS.load_documents(path_to_doesnt_exist_file)


def test_load_documents_on_2_doc(small_dataset):
    expected_docs = small_dataset["list_docs"]

    load_docs = IIS.load_documents(small_dataset["file_with_raw_docs"])

    assert expected_docs == load_docs


def test_load_documents_on_incorrect_doc_struct(tmpdir):
    docs = "1\tHello\n2\tworld\tworld\n"

    file = tmpdir.join("tmp.txt")
    file.write(docs)

    with pytest.raises(ValueError):
        IIS.load_documents(file.strpath)


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
