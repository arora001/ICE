# minimr results

Machine: Linux-6.18.44-fc-v880-x86_64-with-glibc2.39, 2 CPU cores, Python 3.13.18. Timings depend on the machine; ratios are what matter.

## Demo 1: Combiner function (§4.3)

### A. Word count, 2,000,000 Zipf-distributed words, M=16, R=4

| | Combiner off | Combiner on |
|---|---:|---:|
| Records emitted by map | 2,000,000 | 2,000,000 |
| Intermediate records written and shuffled | 2,000,000 | 186,651 |
| Intermediate bytes | 28.5 MB | 2.8 MB |
| Job time | 2.70 s | 0.81 s |
| Output equals sequential Counter | True | True |

Shuffle reduced by **90.7%**. Top words: isorfuel (291,618), cetelono (135,631), fugi (87,096), po (63,595), mibamipo (49,295). With a combiner each map task writes each word at most once, so the shuffle is capped at M x unique words = 16 x 19,989 = 319,824 records, however large the input grows.

### B. Average temperature per city: a combiner must be associative

| City | True mean | no combiner | mean as combiner (wrong) | (sum, count) combiner (fixed) |
|---|---:|---:|---:|---:|
| chennai | 28.92 | 28.92 | 29.10 (+0.18) | 28.92 |
| delhi | 29.31 | 29.31 | 26.00 (-3.30) | 29.31 |
| leh | 5.12 | 5.12 | 6.05 (+0.93) | 5.12 |
| mumbai | 28.06 | 28.06 | 27.25 (-0.80) | 28.06 |
| shimla | 7.97 | 7.97 | 12.27 (+4.30) | 7.97 |

Using `mean` as the combiner averages per-split averages, which weights every split equally regardless of how many readings it holds. Carrying `(sum, count)` is associative and commutative, so partial merging is safe; division happens once in reduce.

## Demo 2: Backup tasks (§3.6)

Worker 0 runs 10x slower. 5 runs per setting, word count, M=16, R=4, 4 workers.

| | Median job time | All runs (s) | Task executions |
|---|---:|---|---:|
| No straggler (reference) | 0.89 s | | 20 |
| Straggler, backups off | 2.55 s | 1.52, 2.55, 3.05, 2.10, 2.99 | 20 |
| Straggler, backups on | 0.93 s | 0.93, 0.97, 0.91, 0.90, 0.99 | 24 |

Without backup tasks the job takes **174% longer** (paper: 44% for its sort job). Backups cost 20% extra task executions. The paper tunes this down to a few percent on thousands of tasks; with only 20 tasks each backup is a large share. Output was identical in every run.

Master log, backups on (excerpt):

```
[  0.690s] assign BACKUP map 0 (attempt 2) -> worker 2
[  0.731s] assign BACKUP map 15 (attempt 2) -> worker 3
[  0.795s] map 0 committed from worker 2, copy on worker(s) [0] now redundant
[  0.812s] map 15 committed from worker 1, copy on worker(s) [3] now redundant
[  0.899s] map 15 finished again on worker 3: ignored
[  0.921s] assign BACKUP reduce 1 (attempt 2) -> worker 1
[  0.927s] reduce 1 committed from worker 2, copy on worker(s) [1] now redundant
[  0.928s] assign BACKUP reduce 3 (attempt 2) -> worker 2
[  0.987s] reduce 3 committed from worker 2, copy on worker(s) [3] now redundant
```

## Demo 3: Worker failure (§3.3)

Worker 2 was killed after finishing 2 map tasks, while running a third. Map task executions: 19 for 16 map tasks. Job time 0.98 s. Output equals sequential Counter: **True**.

Completed map tasks are re-executed too, because their output lived on the dead worker's local disk. Completed reduce tasks would not be, since their output is already in the global file system.

Master log (excerpt):

```
[  0.427s] !! injected failure: killed worker 2
[  0.436s] worker 2 is not responding: marked failed
[  0.437s]   map 10 was in progress there -> idle
[  0.437s]   map 2 was completed there -> idle, will re-execute
[  0.437s]   map 7 was completed there -> idle, will re-execute
[  0.438s] assign map 2 (attempt 2) -> worker 3
[  0.522s] assign map 7 (attempt 2) -> worker 3
[  0.561s] assign map 10 (attempt 2) -> worker 1
```
