"""Correctness tests: python3 -m pytest -q"""

import collections
import os
import re

import pytest

from apps import first_letter_partition
from datagen import make_corpus
from minimr import JobConfig, default_partition, make_splits, read_output, run_job


@pytest.fixture(scope="module")
def corpus(tmp_path_factory):
    path = tmp_path_factory.mktemp("data") / "small.txt"
    make_corpus(str(path), n_words=120_000, vocab_size=3_000, seed=3)
    with open(path, encoding="utf-8") as f:
        truth = collections.Counter(re.findall(r"[a-z]+", f.read().lower()))
    return str(path), dict(truth)


def _job(tmp_path, corpus_path, **kw):
    cfg = dict(inputs=[corpus_path], mapper="apps:wc_map", reducer="apps:wc_reduce",
               M=8, R=3, n_workers=3, work_dir=str(tmp_path / "work"), out_dir=str(tmp_path / "out"))
    cfg.update(kw)
    return run_job(JobConfig(**cfg)), str(tmp_path / "out")


def test_splits_cover_file_on_line_boundaries(corpus):
    path, _ = corpus
    splits = make_splits([path], 8)
    assert splits[0][1] == 0 and splits[-1][2] == os.path.getsize(path)
    data = open(path, "rb").read()
    for (_, s, e), (_, s2, _) in zip(splits, splits[1:]):
        assert e == s2 and data[e - 1:e] == b"\n"


@pytest.mark.parametrize("combiner", [None, "apps:wc_reduce"])
def test_wordcount_matches_sequential(tmp_path, corpus, combiner):
    path, truth = corpus
    res, out = _job(tmp_path, path, combiner=combiner)
    assert read_output(out) == truth
    assert len(res.outputs) == 3


def test_combiner_shrinks_intermediate_data(tmp_path, corpus):
    path, _ = corpus
    off, _ = _job(tmp_path / "a", path)
    on, _ = _job(tmp_path / "b", path, combiner="apps:wc_reduce")
    assert on.counters["map_output_records"] == off.counters["map_output_records"]
    assert on.counters["intermediate_records"] < off.counters["intermediate_records"] / 3


def test_partitions_are_sorted_and_respect_partitioner(tmp_path, corpus):
    path, _ = corpus
    for part, partitioner in ((None, default_partition),
                              ("apps:first_letter_partition", first_letter_partition)):
        res, _ = _job(tmp_path / str(part), path, partitioner=part)
        for r, file in enumerate(res.outputs):
            keys = [line.split("\t")[0] for line in open(file, encoding="utf-8")]
            assert keys == sorted(keys)                       # §4.2
            assert all(partitioner(k, 3) == r for k in keys)  # §4.1


def test_backup_tasks_beat_a_straggler_and_keep_output(tmp_path, corpus):
    path, truth = corpus
    res, out = _job(tmp_path, path, combiner="apps:wc_reduce", slowdown={0: 15}, backup_tasks=True)
    assert read_output(out) == truth
    assert any(rec["backup"] for rec in res.timeline)
    assert res.counters["map_task_executions"] > 8            # duplicates happened...
    assert res.counters["map_input_records"] == sum(1 for _ in open(path))  # ...but counted once


def test_worker_failure_reexecutes_completed_maps(tmp_path, corpus):
    path, truth = corpus
    res, out = _job(tmp_path, path, combiner="apps:wc_reduce", kill_worker=(1, 1))
    assert read_output(out) == truth
    assert any("was completed there" in line for line in res.log)
    assert res.counters["map_task_executions"] > 8
