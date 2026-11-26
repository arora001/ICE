"""
Demo 2 - Backup tasks (§3.6).

Worker 0 is a straggler: it does the same work 10x slower (the paper's example is a bad
disk). We time the job with and without backup tasks over several runs, and draw a
timeline of one run of each. Compare with the paper's Figure 3(a) vs 3(b): +44% without backups.
"""

import os
import statistics

from datagen import make_corpus
from minimr import JobConfig, read_output, run_job
import plots

DATA, RESULTS = "data", "results"
TRIALS, SLOWDOWN, WORKERS = 5, 10, 4


def run(trials=TRIALS, verbose=False):
    corpus = os.path.join(DATA, "corpus.txt")
    if not os.path.exists(corpus):
        make_corpus(corpus)

    baseline = None
    times = {False: [], True: []}
    executions = {False: [], True: []}
    sample = {}
    for trial in range(trials):
        for backups in (False, True):
            out_dir = f"out/backup-{'on' if backups else 'off'}"
            res = run_job(JobConfig(inputs=[corpus], mapper="apps:wc_map", reducer="apps:wc_reduce",
                                    combiner="apps:wc_reduce", M=16, R=4, n_workers=WORKERS,
                                    backup_tasks=backups, slowdown={0: SLOWDOWN},
                                    out_dir=out_dir, verbose=verbose))
            output = read_output(out_dir)
            baseline = baseline or output
            assert output == baseline, "backup tasks changed the output"
            times[backups].append(res.elapsed)
            executions[backups].append(res.counters["map_task_executions"]
                                       + res.counters["reduce_task_executions"])
            sample[backups] = res

    # A run with no straggler, for reference.
    healthy = run_job(JobConfig(inputs=[corpus], mapper="apps:wc_map", reducer="apps:wc_reduce",
                                combiner="apps:wc_reduce", M=16, R=4, n_workers=WORKERS,
                                out_dir="out/backup-healthy"))

    plots.backup_times(times[False], times[True], os.path.join(RESULTS, "backup_times.png"))
    plots.gantt_pair(sample[False], sample[True],
                     ["Backup tasks OFF: everyone waits for worker 0",
                      "Backup tasks ON: idle workers re-run worker 0's task, first copy wins"],
                     WORKERS, os.path.join(RESULTS, "backup_timeline.png"),
                     f"One straggler (worker 0, {SLOWDOWN}x slower), word count, M=16, R=4")
    return times, executions, healthy.elapsed, sample


def report(times, executions, healthy, sample):
    off, on = statistics.median(times[False]), statistics.median(times[True])
    extra = statistics.median(executions[True]) / statistics.median(executions[False]) - 1
    log_lines = [l for l in sample[True].log if "BACKUP" in l or "redundant" in l or "ignored" in l]
    return "\n".join([
        "## Demo 2: Backup tasks (§3.6)",
        "",
        f"Worker 0 runs {SLOWDOWN}x slower. {len(times[False])} runs per setting, word count, M=16, R=4, "
        f"{WORKERS} workers.",
        "",
        "| | Median job time | All runs (s) | Task executions |",
        "|---|---:|---|---:|",
        f"| No straggler (reference) | {healthy:.2f} s | | 20 |",
        f"| Straggler, backups off | {off:.2f} s | {', '.join(f'{t:.2f}' for t in times[False])} | "
        f"{statistics.median(executions[False]):.0f} |",
        f"| Straggler, backups on | {on:.2f} s | {', '.join(f'{t:.2f}' for t in times[True])} | "
        f"{statistics.median(executions[True]):.0f} |",
        "",
        f"Without backup tasks the job takes **{off / on - 1:.0%} longer** (paper: 44% for its sort job). "
        f"Backups cost {extra:.0%} extra task executions. The paper tunes this down to a few percent "
        f"on thousands of tasks; with only 20 tasks each backup is a large share. "
        f"Output was identical in every run.",
        "",
        "Master log, backups on (excerpt):",
        "",
        "```",
        *log_lines,
        "```",
        "",
    ])


if __name__ == "__main__":
    os.makedirs(RESULTS, exist_ok=True)
    print(report(*run(verbose=False)))
