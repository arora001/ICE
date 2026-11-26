"""
Demo 1 - Combiner function (§4.3).

Part A: word count with and without a combiner. Same output, far less intermediate data.
Part B: average temperature per city. Reusing a non-associative reduce (mean) as the
        combiner gives WRONG answers; carrying (sum, count) fixes it.
"""

import collections
import os
import re

from datagen import make_corpus, make_readings
from minimr import JobConfig, read_output, run_job
import plots

DATA, RESULTS = "data", "results"


def wordcount(verbose=False):
    corpus = os.path.join(DATA, "corpus.txt")
    if not os.path.exists(corpus):
        make_corpus(corpus)
    with open(corpus, encoding="utf-8") as f:
        truth = collections.Counter(re.findall(r"[a-z]+", f.read().lower()))

    runs = {}
    for name, combiner in (("off", None), ("on", "apps:wc_reduce")):
        res = run_job(JobConfig(inputs=[corpus], mapper="apps:wc_map", reducer="apps:wc_reduce",
                                combiner=combiner, M=16, R=4, n_workers=4,
                                out_dir=f"out/wc-combiner-{name}", verbose=verbose))
        out = read_output(f"out/wc-combiner-{name}")
        runs[name] = (res, out == dict(truth))

    off, on = runs["off"][0].counters, runs["on"][0].counters
    plots.combiner_bars(off, on, os.path.join(RESULTS, "combiner_intermediate.png"))
    return {
        "map_output_records": off["map_output_records"],
        "records_off": off["intermediate_records"], "records_on": on["intermediate_records"],
        "bytes_off": off["intermediate_bytes"], "bytes_on": on["intermediate_bytes"],
        "time_off": runs["off"][0].elapsed, "time_on": runs["on"][0].elapsed,
        "correct_off": runs["off"][1], "correct_on": runs["on"][1],
        "unique_words": len(truth),
        "top_words": truth.most_common(5),
    }


def average(verbose=False):
    readings = os.path.join(DATA, "readings.csv")
    if not os.path.exists(readings):
        make_readings(readings)
    sums = collections.defaultdict(lambda: [0.0, 0])
    with open(readings, encoding="utf-8") as f:
        for line in f:
            city, t = line.strip().split(",")
            sums[city][0] += float(t)
            sums[city][1] += 1
    truth = {c: round(s / n, 4) for c, (s, n) in sums.items()}

    variants = {
        "no combiner": ("apps:temp_map", "apps:mean_reduce", None),
        "mean as combiner (wrong)": ("apps:temp_map", "apps:mean_reduce", "apps:mean_reduce"),
        "(sum, count) combiner (fixed)": ("apps:temp_map_pair", "apps:mean_from_pairs", "apps:sum_pair_combine"),
    }
    table = {}
    for name, (mapper, reducer, combiner) in variants.items():
        out_dir = "out/avg-" + re.sub(r"[^a-z]+", "-", name).strip("-")
        res = run_job(JobConfig(inputs=[readings], mapper=mapper, reducer=reducer, combiner=combiner,
                                M=8, R=2, n_workers=4, out_dir=out_dir, verbose=verbose))
        table[name] = (read_output(out_dir), res.counters["intermediate_records"])
    return truth, table


def report(wc, avg):
    truth, table = avg
    names = list(table)
    lines = [
        "## Demo 1: Combiner function (§4.3)",
        "",
        "### A. Word count, 2,000,000 Zipf-distributed words, M=16, R=4",
        "",
        "| | Combiner off | Combiner on |",
        "|---|---:|---:|",
        f"| Records emitted by map | {wc['map_output_records']:,} | {wc['map_output_records']:,} |",
        f"| Intermediate records written and shuffled | {wc['records_off']:,} | {wc['records_on']:,} |",
        f"| Intermediate bytes | {wc['bytes_off'] / 1e6:.1f} MB | {wc['bytes_on'] / 1e6:.1f} MB |",
        f"| Job time | {wc['time_off']:.2f} s | {wc['time_on']:.2f} s |",
        f"| Output equals sequential Counter | {wc['correct_off']} | {wc['correct_on']} |",
        "",
        f"Shuffle reduced by **{1 - wc['records_on'] / wc['records_off']:.1%}**. "
        f"Top words: {', '.join(f'{w} ({c:,})' for w, c in wc['top_words'])}. "
        f"With a combiner each map task writes each word at most once, so the shuffle is capped at "
        f"M x unique words = 16 x {wc['unique_words']:,} = {16 * wc['unique_words']:,} records, "
        f"however large the input grows.",
        "",
        "### B. Average temperature per city: a combiner must be associative",
        "",
        "| City | True mean | " + " | ".join(names) + " |",
        "|---|---:|" + "---:|" * len(names),
    ]
    for city in sorted(truth):
        cells = []
        for n in names:
            v = table[n][0][city]
            err = v - truth[city]
            cells.append(f"{v:.2f}" + (f" ({err:+.2f})" if abs(err) > 0.005 else ""))
        lines.append(f"| {city} | {truth[city]:.2f} | " + " | ".join(cells) + " |")
    lines += [
        "",
        "Using `mean` as the combiner averages per-split averages, which weights every split "
        "equally regardless of how many readings it holds. Carrying `(sum, count)` is "
        "associative and commutative, so partial merging is safe; division happens once in reduce.",
        "",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    os.makedirs(RESULTS, exist_ok=True)
    print(report(wordcount(), average()))
