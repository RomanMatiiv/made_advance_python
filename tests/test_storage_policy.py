import zlib

import pytest

from storage_policy import JsonStoragePolicy
from storage_policy import PklStoragePolicy
from storage_policy import ZlibStoragePolicy


@pytest.fixture()
def sample_inverted_index(tmpdir):
    inverted_index = {"hello": [1, 4],
                      "world": [1],
                      "I": [3, 4],
                      "am": [2],
                      "human": [2, 3]}
    return inverted_index


def test_json_storage_policy(tmpdir, sample_inverted_index):
    expect = sample_inverted_index

    path_to_dump = tmpdir.join("dump_tmp").strpath

    JsonStoragePolicy.dump(expect, path_to_dump)

    real = JsonStoragePolicy.load(path_to_dump)

    assert expect == real


def test_pkl_storage_policy(tmpdir, sample_inverted_index):
    expect = sample_inverted_index

    path_to_dump = tmpdir.join("dump_tmp").strpath

    PklStoragePolicy.dump(expect, path_to_dump)

    real = PklStoragePolicy.load(path_to_dump)

    assert expect == real


def test_zlib_storage_policy(tmpdir, sample_inverted_index):
    expect = sample_inverted_index

    path_to_dump = tmpdir.join("dump_tmp").strpath

    ZlibStoragePolicy.dump(expect, path_to_dump)

    real = ZlibStoragePolicy.load(path_to_dump)

    assert expect == real


def test_zlib_storage_policy_compress_level(tmpdir, sample_inverted_index):
    expect = sample_inverted_index

    path_to_dump = tmpdir.join("dump_tmp").strpath

    ZlibStoragePolicy.dump(expect, path_to_dump, level=9)

    real = ZlibStoragePolicy.load(path_to_dump)

    assert expect == real


def test_zlib_storage_policy_raise_compress_level(tmpdir, sample_inverted_index):
    path_to_dump = tmpdir.join("dump_tmp").strpath

    with pytest.raises(zlib.error):
        ZlibStoragePolicy.dump(sample_inverted_index, path_to_dump, level=10)
