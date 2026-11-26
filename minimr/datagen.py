"""Synthetic inputs for the demos (deterministic: same seed, same files)."""

import os

import numpy as np

_SYLLABLES = ["ka", "lo", "mi", "ra", "te", "su", "no", "vi", "de", "po",
              "an", "el", "is", "or", "un", "ba", "ce", "fu", "gi", "ho"]


def _vocabulary(n, rng):
    words, seen = [], set()
    while len(words) < n:
        w = "".join(rng.choice(_SYLLABLES, size=rng.integers(1, 5)))
        if w not in seen:
            seen.add(w)
            words.append(w)
    return words


def make_corpus(path, n_words=2_000_000, vocab_size=20_000, zipf_s=1.1, seed=7):
    """Text whose word frequencies follow a Zipf law, like real text (§4.3 relies on
    this: a handful of words such as "the" make up a large share of all records)."""
    rng = np.random.default_rng(seed)
    vocab = np.array(_vocabulary(vocab_size, rng))
    ranks = np.arange(1, vocab_size + 1)
    probs = 1.0 / ranks ** zipf_s
    probs /= probs.sum()
    words = vocab[rng.choice(vocab_size, size=n_words, p=probs)]
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        i = 0
        while i < n_words:
            n = int(rng.integers(8, 16))
            f.write(" ".join(words[i:i + n]) + "\n")
            i += n
    return path


def make_readings(path, n_days=365, seed=11):
    """City temperature log in date order. Cities report at different rates in
    different seasons, so each input split holds a different number of readings
    per city - exactly the situation where a mean-of-means is wrong."""
    rng = np.random.default_rng(seed)
    cities = {  # name: (base temp, seasonal swing, peak-reporting season offset)
        "delhi": (25, 10, 0.0), "shimla": (12, 9, 0.5), "chennai": (29, 3, 0.25),
        "leh": (5, 14, 0.75), "mumbai": (27, 3, 0.1),
    }
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for day in range(n_days):
            season = np.sin(2 * np.pi * day / n_days)
            for city, (base, swing, phase) in cities.items():
                rate = 6 + 5 * np.sin(2 * np.pi * (day / n_days + phase))   # 1..11 per day
                for _ in range(rng.poisson(rate)):
                    temp = base + swing * season + rng.normal(0, 2)
                    f.write(f"{city},{temp:.2f}\n")
    return path
