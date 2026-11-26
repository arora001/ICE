"""
Demo 3 - Worker failure and re-execution (§3.3).

Worker 2 is killed (SIGKILL) part-way through the map phase. The master notices,
throws away the map output on that worker's "local disk", resets both its in-progress
and its already-completed map tasks to idle, re-runs them on the survivors, and the
job still produces exactly the right answer.
"""

import collections
import os
import re

from datagen import make_corpus
from minimr import JobConfig, read_output, run_job
import plots

DATA, RESULTS = "data", "results"
VICTIM, AFTER_MAPS, WORKERS = 2, 2, 4


def run(verbose=False):
    corpus = os.path.join(DATA, "corpus.txt")
    if not os.path.exists(corpus):
        make_corpus(corpus)
    with open(corpus, encoding="utf-8") as f:
        truth = collections.Counter(re.findall(r"[a-z]+", f.read().lower()))

    res = run_job(JobConfig(inputs=[corpus], mapper="apps:wc_map", reducer="apps:wc_reduce",
                            combiner="apps:wc_reduce", M=16, R=4, n_workers=WORKERS,
                            kill_worker=(VICTIM, AFTER_MAPS), out_dir="out/fault",
                            verbose=verbose))
    correct = read_output("out/fault") == dict(truth)
    plots.gantt_single(res, WORKERS, os.path.join(RESULTS, "fault_timeline.png"),
                       f"Worker {VICTIM} killed after {AFTER_MAPS} map tasks: its finished work is re-run")
    return res, correct


def report(res, correct):
    key = [l for l in res.log if "killed" in l or "not responding" in l or "->  idle" in l
           or "-> idle" in l or "attempt 2" in l]
    return "\n".join([
        "## Demo 3: Worker failure (§3.3)",
        "",
        f"Worker {VICTIM} was killed after finishing {AFTER_MAPS} map tasks, while running a third. "
        f"Map task executions: {res.counters['map_task_executions']} for {res.n_map} map tasks. "
        f"Job time {res.elapsed:.2f} s. Output equals sequential Counter: **{correct}**.",
        "",
        "Completed map tasks are re-executed too, because their output lived on the dead "
        "worker's local disk. Completed reduce tasks would not be, since their output is "
        "already in the global file system.",
        "",
        "Master log (excerpt):",
        "",
        "```",
        *key,
        "```",
        "",
    ])


if __name__ == "__main__":
    os.makedirs(RESULTS, exist_ok=True)
    print(report(*run()))
