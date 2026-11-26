import hashlib

import pytest

from conftest import SAMPLE_AUG, SAMPLE_JUL
from etl.batching import hash_line_ranges, write_batches


def test_batches_are_unchanged_bytes_and_never_join_files(sample_files, tmp_path):
    batches = list(write_batches(sample_files, 4, tmp_path / "work"))
    # July has 7 lines -> batches of 4 and 3; August has 4 lines -> one batch of 4.
    assert [(b.batch_id, b.source_file.name, b.first_line, b.last_line, b.record_count)
            for b in batches] == [(1, "jul.log", 1, 4, 4), (2, "jul.log", 5, 7, 3), (3, "aug.log", 1, 4, 4)]
    jul = b"".join(b.path.read_bytes() for b in batches[:2])
    aug = batches[2].path.read_bytes()
    assert jul == SAMPLE_JUL and aug == SAMPLE_AUG          # byte for byte, nothing added
    assert batches[1].path.read_bytes().endswith(b"alyssa.p")  # cut-off line kept as is
    assert b"\r\n" in aug                                    # \r left for the pipeline


def test_batch_ids_continue_across_files(sample_files, tmp_path):
    batches = list(write_batches(sample_files, 100, tmp_path / "work"))
    assert [(b.batch_id, b.record_count) for b in batches] == [(1, 7), (2, 4)]


def test_hashes_match_the_batch_bytes_and_the_source_ranges(sample_files, tmp_path):
    batches = list(write_batches(sample_files, 3, tmp_path / "work"))
    for b in batches:
        assert b.sha256 == hashlib.sha256(b.path.read_bytes()).hexdigest()
    jul = [b for b in batches if b.source_file == sample_files[0]]
    assert hash_line_ranges(sample_files[0], [(b.first_line, b.last_line) for b in jul]) == [b.sha256 for b in jul]


def test_range_past_end_of_file_has_no_hash(sample_files):
    assert hash_line_ranges(sample_files[1], [(1, 2), (3, 99)])[1] is None


def test_empty_input_gives_no_batches(tmp_path):
    empty = tmp_path / "empty.log"
    empty.write_bytes(b"")
    assert list(write_batches([empty], 5, tmp_path / "work")) == []


def test_batch_size_must_be_positive(sample_files, tmp_path):
    with pytest.raises(ValueError):
        list(write_batches(sample_files, 0, tmp_path / "work"))
