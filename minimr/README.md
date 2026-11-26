# minimr: a mini MapReduce

A single-machine re-implementation of the core of Google's MapReduce paper (Dean & Ghemawat, OSDI 2004).
It uses Python processes as workers and a master that schedules them. The headline module is the
**combiner (§4.3)**, demonstrated alongside **backup tasks (§3.6)** and **worker-failure re-execution (§3.3)**.

## Run it

```bash
pip install numpy matplotlib pytest
python3 run_all.py          # all three demos (~45 s), writes results/RESULTS.md and results/*.png
python3 demo_combiner.py    # or one demo at a time
python3 demo_backup.py
python3 demo_fault.py
python3 -m pytest -q        # 7 correctness tests
```

Needs Python 3.9 or later. Works on Linux, macOS and Windows because it uses the `spawn` start method.

## Files

| File | What it is |
|---|---|
| `minimr.py` | The library: master, workers, splitting, partitioning, shuffle, sort, combiner, backup tasks, failure handling, counters |
| `apps.py` | User code: word count, average temperature (wrong and fixed combiner), a custom partitioner |
| `datagen.py` | Deterministic synthetic inputs: a Zipf-distributed corpus and a city temperature log |
| `demo_combiner.py` | Demo 1: shuffle size with and without a combiner, plus a combiner that gives wrong answers |
| `demo_backup.py` | Demo 2: one worker 10x slower, job time with and without backup tasks |
| `demo_fault.py` | Demo 3: a worker is killed mid-job and the job still gives the right answer |
| `plots.py` | Charts |
| `test_minimr.py` | Tests against a plain sequential `Counter` |

## How it maps to the paper

| Paper | Here |
|---|---|
| §2 `map(k1,v1) → list(k2,v2)`, `reduce(k2, list(v2))` | Plain Python functions in `apps.py`, named as `"apps:wc_map"`, like `REGISTER_MAPPER` |
| §3.1 split input into M pieces, R reduce partitions | `make_splits` cuts files into ~M byte ranges that end on line boundaries. Each map writes R files |
| §3.1 intermediate data on the map worker's local disk | `work/worker-<id>/mr-<map>-<reduce>-a<attempt>.jsonl` |
| §3.1 output in a global file system | `out/part-0000r-of-0000R` |
| §3.2 master keeps state + worker per task | `_Task.state` is idle / in-progress / completed; `running` and `winner` record the workers |
| §3.3 ping workers, re-run their tasks | `Process.is_alive()` stands in for the ping. Both the in-progress map tasks and the already-completed map tasks of a dead worker go back to idle, and its local directory is deleted |
| §3.3 ignore duplicate completions, atomic rename | The master ignores a second "done" for a completed task. Reduce output is written to a temp file and committed with `os.replace` |
| §3.6 backup tasks | When a phase has no idle tasks left, an idle worker gets a copy of the longest-running in-progress task. The first copy to finish wins |
| §4.1 `hash(key) mod R` | `crc32(key) mod R`. Python's own `hash()` is randomised per process, so workers would disagree on where a key goes |
| §4.2 keys in increasing order per partition | Reduce sorts its input before grouping |
| §4.3 combiner | The reduce signature, run on each map task's buffered output before it is written |
| §4.4 text input: key = offset, value = line | Yes |
| §4.9 counters, duplicates not double counted | Summed from the committed execution of each task only |

## Results (2-core cloud machine; see `results/RESULTS.md`)

- **Combiner:** shuffled records fall from 2,000,000 to 186,651 (−91%) and the job gets about 3x faster.
  The output is identical.
- **A wrong combiner:** reusing `mean` as the combiner is off by up to 4.3 °C. Carrying `(sum, count)`
  fixes it exactly. A combiner is only safe if the reduce is associative and commutative and its output
  has the same type as the map output.
- **Backup tasks:** with one worker 10x slower, the median job time is 2.55 s without backups and
  0.93 s with them, about the same as a job with no straggler (0.89 s). Backups cost 20% extra task
  executions here. Exact times vary from run to run; the gap does not.
- **Worker failure:** a worker is killed after 2 map tasks, and those 2 plus the one it was running are
  re-executed (19 map executions for 16 tasks). The output is still exactly right.

## Points worth making in a presentation

1. **The combiner's benefit has a ceiling set by the data, not the input size.** After combining, each map
   task emits each distinct key at most once. Shuffle volume is therefore at most M × distinct keys,
   however large the input grows. That bound is why it works so well on Zipf-distributed data like text.
2. **"Same code as reduce" is a trap.** The paper says the combiner is typically the reduce function. That is
   only valid for associative, commutative reduces. Mean, median and "top-k by count after filtering" all
   break. Demo 1B shows the error.
3. **Backups don't make the slow machine faster; they make it irrelevant.** The straggler still runs to the
   end (the hatched bar in `backup_timeline.png`). The job just stops waiting for it.
4. **The overhead depends on task count.** With 20 tasks, 4 backups are 20% extra work. The paper reports
   "a few percent" with M = 200,000 tasks. Many small tasks make backups cheap, which is the paper's
   §3.5 argument for M ≫ number of machines.
5. **Completed map work is more fragile than completed reduce work.** Map output lives on local disk and
   dies with the machine. Reduce output is in the replicated file system. The paper trades extra
   re-execution for network bandwidth, which it calls the scarce resource.

## Deliberate simplifications

- One machine. Workers are processes, "local disks" are directories and the "network" is the shared
  filesystem. There is no locality scheduling (§3.4) because there is no network topology to exploit.
- Map and reduce phases are separated by a barrier. In the paper, reduce workers start fetching as soon as
  the first maps finish; only the reduce function waits for all of them.
- A straggler is simulated by sleeping (factor − 1) × its real compute time, which mirrors a slow disk.
- Failure detection uses `is_alive()` rather than a heartbeat timeout. A frozen but alive worker would be
  caught only by a backup task, not by failure detection.
- Master failure is not handled. The paper doesn't handle it either; it aborts the job.
