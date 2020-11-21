import zlib

import pytest

from storage_policy import JsonStoragePolicy
from storage_policy import PklStoragePolicy
from storage_policy import ZlibStoragePolicy
from storage_policy import StructStoragePolicy

@pytest.fixture()
def sample_inverted_index(tmpdir):
    inverted_index = {"hello": [1, 4],
                      "world": [1],
                      "I": [3, 4],
                      "am": [2],
                      "human": [2, 3],
                      "россия": [1, 2, 3, 4]}
    return inverted_index


def test_json_storage_policy(tmpdir, sample_inverted_index):
    expect = sample_inverted_index

    path_to_dump = tmpdir.join("dump_tmp").strpath

    storage_policy = JsonStoragePolicy()
    storage_policy.dump(expect, path_to_dump)

    storage_policy = JsonStoragePolicy()
    real = storage_policy.load(path_to_dump)

    assert expect == real


def test_pkl_storage_policy(tmpdir, sample_inverted_index):
    expect = sample_inverted_index

    path_to_dump = tmpdir.join("dump_tmp").strpath

    storage_policy = PklStoragePolicy()
    storage_policy.dump(expect, path_to_dump)

    storage_policy = PklStoragePolicy()
    real = storage_policy.load(path_to_dump)

    assert expect == real


def test_zlib_storage_policy(tmpdir, sample_inverted_index):
    expect = sample_inverted_index

    path_to_dump = tmpdir.join("dump_tmp").strpath

    storage_policy = ZlibStoragePolicy(encoding="cp1251")
    storage_policy.dump(expect, path_to_dump)

    storage_policy = ZlibStoragePolicy(encoding="utf8")

    real = storage_policy.load(path_to_dump)

    assert expect == real


def test_zlib_storage_policy_compress_level(tmpdir, sample_inverted_index):
    expect = sample_inverted_index

    path_to_dump = tmpdir.join("dump_tmp").strpath

    storage_policy = ZlibStoragePolicy(encoding="utf8", level=9)
    storage_policy.dump(expect, path_to_dump, level=9)

    storage_policy = ZlibStoragePolicy(encoding="utf8", level=9)
    real = storage_policy.load(path_to_dump)

    assert expect == real


def test_zlib_storage_policy_raise_compress_level(tmpdir, sample_inverted_index):
    path_to_dump = tmpdir.join("dump_tmp").strpath

    storage_policy = ZlibStoragePolicy(encoding="utf8", level=10)

    with pytest.raises(zlib.error):
        storage_policy.dump(sample_inverted_index, path_to_dump, level=10)


def test_struct_storage_policy(tmpdir, sample_inverted_index):
    expect = sample_inverted_index

    path_to_dump = tmpdir.join("dump_tmp").strpath

    storage_policy = StructStoragePolicy(encoding="utf8")
    storage_policy.dump(expect, path_to_dump)

    storage_policy = StructStoragePolicy(encoding="utf8")
    real = storage_policy.load(path_to_dump)

    assert expect == real
