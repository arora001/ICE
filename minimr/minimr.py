"""
minimr - a small, single-machine re-implementation of Google's MapReduce
(Dean & Ghemawat, "MapReduce: Simplified Data Processing on Large Clusters", OSDI 2004).

Workers are OS processes; the master runs in the parent process. Each worker has
its own directory under work_dir that plays the part of the worker's "local disk".
The output directory plays the part of the global file system (GFS).

What is implemented, by paper section:
  2      map(k1, v1) -> list(k2, v2) and reduce(k2, list(v2)) -> v2, written by the user
  3.1    input split into ~M pieces at line boundaries, R reduce partitions, R output files
  3.2    master keeps a state (idle / in-progress / completed) and a worker for every task
  3.3    worker failure: dead worker detected, its in-progress AND completed map tasks
         re-executed, in-progress reduce tasks re-executed; duplicate completions
         ignored; reduce output committed with an atomic rename
  3.6    backup tasks: when a phase has no idle tasks left, idle workers run backup
         copies of in-progress tasks and whichever copy finishes first wins
  4.1    partitioning function, default hash(key) mod R (a stable hash, see below)
  4.2    keys are processed in increasing order inside every partition
  4.3    combiner: partial reduce on the map side before intermediate data is written
  4.4    "text" input: key = byte offset of the line, value = the line
  4.9    counters, aggregated only from the committed execution of each task
"""

from __future__ import annotations

import importlib
import json
import multiprocessing as mp
import multiprocessing.connection as mp_connection
import os
import shutil
import time
import zlib
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import groupby
from operator import itemgetter
from typing import Optional

IDLE, IN_PROGRESS, COMPLETED = "idle", "in-progress", "completed"


# --------------------------------------------------------------------------- helpers

def load(spec: Optional[str]):
    """Load a user function from "module:function" (the paper's REGISTER_MAPPER)."""
    if spec is None:
        return None
    module, name = spec.split(":")
    return getattr(importlib.import_module(module), name)


def default_partition(key, R: int) -> int:
    """hash(key) mod R. Python's built-in hash() of a str is randomised per
    process, so two workers would disagree on where a key goes; crc32 is stable."""
    return zlib.crc32(str(key).encode("utf-8")) % R


def make_splits(paths, M: int):
    """Cut the input files into about M byte ranges that end on line boundaries (§4.4)."""
    total = sum(os.path.getsize(p) for p in paths)
    target = max(1, total // M)
    splits = []
    for path in paths:
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            start = 0
            while start < size:
                end = min(size, start + target)
                if end < size:
                    f.seek(end)
                    f.readline()          # move the cut to the end of the current line
                    end = f.tell()
                splits.append((path, start, end))
                start = end
    return splits


def read_output(out_dir: str) -> dict:
    """Read every part-* file of a finished job into one {key: value} dict."""
    result = {}
    for name in sorted(os.listdir(out_dir)):
        if name.startswith("part-"):
            with open(os.path.join(out_dir, name), encoding="utf-8") as f:
                for line in f:
                    key, value = line.rstrip("\n").split("\t", 1)
                    result[key] = json.loads(value)
    return result


# --------------------------------------------------------------------------- job description

@dataclass
class JobConfig:
    inputs: list                       # input file paths
    mapper: str                        # "module:function"
    reducer: str                       # "module:function"
    combiner: Optional[str] = None     # "module:function", usually the reducer itself
    partitioner: Optional[str] = None  # "module:function(key, R) -> int"
    M: int = 16                        # target number of map tasks
    R: int = 4                         # number of reduce tasks = output files
    n_workers: int = 4
    work_dir: str = "work"             # worker-<id>/ inside it = that worker's local disk
    out_dir: str = "out"               # the "global file system"
    backup_tasks: bool = False         # §3.6
    slowdown: dict = field(default_factory=dict)       # {worker_id: factor} -> straggler
    kill_worker: Optional[tuple] = None  # (worker_id, n): kill it once it has finished
                                         # n map tasks and been handed its next one
    verbose: bool = False


@dataclass
class JobResult:
    elapsed: float                     # seconds, first assignment -> last reduce committed
    counters: dict
    timeline: list                     # one dict per task execution (attempt)
    log: list                          # master's event log
    outputs: list                      # paths of the R output files
    n_map: int
    n_reduce: int


# --------------------------------------------------------------------------- worker side

def _do_map(task_id, attempt, split, R, mapper, combiner, partitioner, local_dir, stall):
    path, start, end = split
    counters = defaultdict(int)
    buckets = [[] for _ in range(R)]             # intermediate pairs buffered in memory

    with open(path, "rb") as f:
        f.seek(start)
        offset = start
        while offset < end:
            raw = f.readline()
            if not raw:
                break
            counters["map_input_records"] += 1
            for key, value in mapper(offset, raw.decode("utf-8").rstrip("\n")):
                buckets[partitioner(key, R)].append((key, value))
                counters["map_output_records"] += 1
            offset += len(raw)

    files, tmp_files = [], []
    for r, pairs in enumerate(buckets):
        if combiner is not None:                 # §4.3: partial merge before the "network"
            grouped = defaultdict(list)
            for key, value in pairs:
                grouped[key].append(value)
            pairs = [(key, combiner(key, values)) for key, values in grouped.items()]
        tmp = os.path.join(local_dir, f".tmp-mr-{task_id}-{r}-a{attempt}")
        with open(tmp, "w", encoding="utf-8") as out:
            for key, value in pairs:
                out.write(json.dumps([key, value]) + "\n")
        counters["intermediate_records"] += len(pairs)
        counters["intermediate_bytes"] += os.path.getsize(tmp)
        tmp_files.append(tmp)
        files.append(os.path.join(local_dir, f"mr-{task_id}-{r}-a{attempt}.jsonl"))

    stall()                                      # a slow machine is slow before it reports
    for tmp, final in zip(tmp_files, files):
        os.replace(tmp, final)
    return {"files": files, "counters": dict(counters)}


def _do_reduce(task_id, attempt, files, R, reducer, out_dir, worker_id, stall):
    counters = defaultdict(int)
    pairs = []
    for path in files:                           # the "remote read" of each map's region r
        with open(path, encoding="utf-8") as f:
            for line in f:
                key, value = json.loads(line)
                pairs.append((key, value))
    counters["reduce_input_records"] = len(pairs)

    pairs.sort(key=itemgetter(0))                # §4.2 ordering guarantee
    tmp = os.path.join(out_dir, f".tmp-part-{task_id}-w{worker_id}-a{attempt}")
    with open(tmp, "w", encoding="utf-8") as out:
        for key, group in groupby(pairs, key=itemgetter(0)):
            value = reducer(key, [v for _, v in group])
            out.write(f"{key}\t{json.dumps(value)}\n")
            counters["reduce_output_records"] += 1

    stall()
    final = os.path.join(out_dir, f"part-{task_id:05d}-of-{R:05d}")
    os.replace(tmp, final)                       # §3.3 atomic commit
    return {"file": final, "counters": dict(counters)}


def _worker_main(worker_id, conn, cfg: dict):
    local_dir = os.path.join(cfg["work_dir"], f"worker-{worker_id}")
    os.makedirs(local_dir, exist_ok=True)
    mapper, reducer = load(cfg["mapper"]), load(cfg["reducer"])
    combiner = load(cfg["combiner"])
    partitioner = load(cfg["partitioner"]) or default_partition
    factor = float(cfg["slowdown"].get(worker_id, 1.0))
    conn.send(("ready", worker_id))

    while True:
        try:
            message = conn.recv()
        except EOFError:                         # master went away
            return
        if message is None:
            return
        kind, task_id, attempt, payload = message
        started = time.perf_counter()

        def stall():
            # A straggler does the same work, only `factor` times slower
            # (the paper's example: a bad disk dropping reads from 30 MB/s to 1 MB/s).
            if factor > 1:
                time.sleep((time.perf_counter() - started) * (factor - 1))

        try:
            if kind == "map":
                result = _do_map(task_id, attempt, payload, cfg["R"], mapper, combiner,
                                 partitioner, local_dir, stall)
            else:
                result = _do_reduce(task_id, attempt, payload, cfg["R"], reducer,
                                    cfg["out_dir"], worker_id, stall)
            conn.send(("done", worker_id, kind, task_id, attempt, True, result))
        except FileNotFoundError as exc:         # a map worker's disk vanished mid-read
            conn.send(("done", worker_id, kind, task_id, attempt, False, str(exc)))


# --------------------------------------------------------------------------- master side

class _Task:
    def __init__(self, kind, task_id, payload=None):
        self.kind, self.id, self.payload = kind, task_id, payload
        self.state = IDLE
        self.running = set()      # workers currently executing a copy of this task
        self.attempts = 0
        self.backed_up = False
        self.output = None        # result of the committed execution
        self.winner = None        # worker whose execution was committed


class Master:
    def __init__(self, cfg: JobConfig):
        self.cfg = cfg
        self.log = []
        self.timeline = {}        # (kind, task_id, attempt) -> record

    # ---- logging -------------------------------------------------------------
    def _now(self):
        return time.perf_counter() - self.t0

    def _say(self, text):
        line = f"[{self._now():7.3f}s] {text}"
        self.log.append(line)
        if self.cfg.verbose:
            print(line, flush=True)

    # ---- main loop -----------------------------------------------------------
    def run(self) -> JobResult:
        cfg = self.cfg
        for d in (cfg.work_dir, cfg.out_dir):
            shutil.rmtree(d, ignore_errors=True)
            os.makedirs(d)

        splits = make_splits(cfg.inputs, cfg.M)
        self.maps = [_Task("map", i, s) for i, s in enumerate(splits)]
        self.reduces = [_Task("reduce", r) for r in range(cfg.R)]

        # One private pipe per worker. A shared queue would let a killed worker die while
        # holding the queue's lock and freeze everybody else.
        ctx = mp.get_context("spawn")            # same behaviour on Linux, macOS, Windows
        self.conns, self.procs = {}, {}
        worker_cfg = {k: getattr(cfg, k) for k in
                      ("mapper", "reducer", "combiner", "partitioner", "R",
                       "work_dir", "out_dir", "slowdown")}
        for wid in range(cfg.n_workers):
            master_end, worker_end = ctx.Pipe()
            p = ctx.Process(target=_worker_main, args=(wid, worker_end, worker_cfg), daemon=True)
            p.start()
            worker_end.close()
            self.conns[wid], self.procs[wid] = master_end, p
        for conn in self.conns.values():         # start the clock once everyone is up
            conn.recv()

        self.idle = set(range(cfg.n_workers))
        self.dead = set()
        self.completed_maps_by = defaultdict(int)
        self.killed = False
        self.t0 = time.perf_counter()
        self._say(f"job start: M={len(self.maps)} map tasks, R={cfg.R} reduce tasks, "
                  f"{cfg.n_workers} workers, backup_tasks={cfg.backup_tasks}, "
                  f"combiner={'on' if cfg.combiner else 'off'}")

        try:
            while not all(t.state == COMPLETED for t in self.reduces):
                self._detect_failures()
                if len(self.dead) == cfg.n_workers:
                    raise RuntimeError("every worker has died")
                self._schedule()
                live = [self.conns[w] for w in self.procs if w not in self.dead]
                for conn in mp_connection.wait(live, timeout=0.01):
                    try:
                        message = conn.recv()
                    except (EOFError, OSError):  # worker died; _detect_failures handles it
                        continue
                    self._handle(message)
            elapsed = self._now()
            self._say("job done")
        finally:
            self._shutdown()

        for rec in self.timeline.values():       # copies nobody waited for
            if rec["end"] is None:
                rec["end"], rec["outcome"] = elapsed, "abandoned"

        counters = defaultdict(int)
        for t in self.maps + self.reduces:       # §4.9: committed executions only
            for k, v in t.output["counters"].items():
                counters[k] += v
        counters["map_task_executions"] = sum(t.attempts for t in self.maps)
        counters["reduce_task_executions"] = sum(t.attempts for t in self.reduces)

        return JobResult(elapsed=elapsed, counters=dict(counters),
                         timeline=sorted(self.timeline.values(), key=itemgetter("start")),
                         log=self.log, outputs=[t.output["file"] for t in self.reduces],
                         n_map=len(self.maps), n_reduce=len(self.reduces))

    # ---- scheduling ----------------------------------------------------------
    def _next_task(self, wid):
        maps_done = all(t.state == COMPLETED for t in self.maps)
        phase = self.reduces if maps_done else self.maps   # reduces need every map's output
        for t in phase:
            if t.state == IDLE:
                return t, False
        if self.cfg.backup_tasks:                # §3.6: nothing new to hand out, so back up
            candidates = [t for t in phase if t.state == IN_PROGRESS and not t.backed_up
                          and len(t.running) == 1 and wid not in t.running]
            if candidates:
                oldest = min(candidates, key=lambda t: self._started_at(t))
                return oldest, True
        return None, False

    def _started_at(self, task):
        return min(rec["start"] for (k, i, _), rec in self.timeline.items()
                   if k == task.kind and i == task.id and rec["end"] is None)

    def _schedule(self):
        for wid in sorted(self.idle):
            task, is_backup = self._next_task(wid)
            if task is None:
                continue
            task.attempts += 1
            task.state = IN_PROGRESS
            task.running.add(wid)
            task.backed_up = task.backed_up or is_backup
            self.idle.discard(wid)
            if task.kind == "map":
                payload = task.payload
            else:
                payload = [m.output["files"][task.id] for m in self.maps]
            self.timeline[(task.kind, task.id, task.attempts)] = {
                "kind": task.kind, "task": task.id, "attempt": task.attempts,
                "worker": wid, "backup": is_backup, "start": self._now(),
                "end": None, "outcome": None}
            try:
                self.conns[wid].send((task.kind, task.id, task.attempts, payload))
            except OSError:
                pass                             # died just now; failure detection resets it
            self._say(f"assign {'BACKUP ' if is_backup else ''}{task.kind} {task.id} "
                      f"(attempt {task.attempts}) -> worker {wid}")

            kw = self.cfg.kill_worker
            if (kw and not self.killed and wid == kw[0]
                    and self.completed_maps_by[wid] >= kw[1]):
                self.killed = True
                self.procs[wid].kill()           # failure injection
                self._say(f"!! injected failure: killed worker {wid}")

    # ---- completion messages -------------------------------------------------
    def _handle(self, message):
        _, wid, kind, task_id, attempt, ok, result = message
        if wid in self.dead:
            return                               # message from a machine we've written off
        self.idle.add(wid)
        task = (self.maps if kind == "map" else self.reduces)[task_id]
        task.running.discard(wid)
        rec = self.timeline[(kind, task_id, attempt)]
        rec["end"] = self._now()

        if not ok:
            rec["outcome"] = "failed"
            self._say(f"{kind} {task_id} on worker {wid} failed ({result})")
            if task.state == IN_PROGRESS and not task.running:
                task.state = IDLE
            return

        if task.state == COMPLETED:              # §3.3: ignore a second completion
            rec["outcome"] = "duplicate (ignored)"
            self._say(f"{kind} {task_id} finished again on worker {wid}: ignored")
            return

        task.state, task.output, task.winner = COMPLETED, result, wid
        rec["outcome"] = "committed"
        if kind == "map":
            self.completed_maps_by[wid] += 1
        extra = f", copy on worker(s) {sorted(task.running)} now redundant" if task.running else ""
        self._say(f"{kind} {task_id} committed from worker {wid}{extra}")

    # ---- failure detection (§3.3) ----------------------------------------------
    def _detect_failures(self):
        for wid, proc in self.procs.items():
            if wid in self.dead or proc.is_alive():   # is_alive() stands in for the ping
                continue
            self.dead.add(wid)
            self.idle.discard(wid)
            self._say(f"worker {wid} is not responding: marked failed")
            # Its local disk is now unreachable, so its map output is gone.
            shutil.rmtree(os.path.join(self.cfg.work_dir, f"worker-{wid}"), ignore_errors=True)

            for t in self.maps + self.reduces:
                if wid in t.running:
                    t.running.discard(wid)
                    for (k, i, _), rec in self.timeline.items():
                        if k == t.kind and i == t.id and rec["worker"] == wid and rec["end"] is None:
                            rec["end"], rec["outcome"] = self._now(), "lost (worker died)"
                    if not t.running and t.state == IN_PROGRESS:
                        t.state, t.backed_up = IDLE, False
                        self._say(f"  {t.kind} {t.id} was in progress there -> idle")
            for t in self.maps:
                if t.state == COMPLETED and t.winner == wid:
                    t.state, t.output, t.winner, t.backed_up = IDLE, None, None, False
                    self._say(f"  map {t.id} was completed there -> idle, will re-execute")

    def _shutdown(self):
        for wid, proc in self.procs.items():
            if proc.is_alive():
                try:
                    self.conns[wid].send(None)
                except OSError:
                    pass
        deadline = time.time() + 0.5
        for proc in self.procs.values():
            proc.join(timeout=max(0.0, deadline - time.time()))
            if proc.is_alive():
                proc.terminate()                 # e.g. a straggler whose work was not needed
                proc.join()
        for conn in self.conns.values():
            conn.close()


def run_job(cfg: JobConfig) -> JobResult:
    return Master(cfg).run()
