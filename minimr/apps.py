"""
User code: the only part a MapReduce user writes. Everything here is plain functions;
the library is told about them by name ("apps:wc_map"), like REGISTER_MAPPER in the paper.

map(offset, line)  -> yields (key, value) pairs
reduce(key, values) -> one value
A combiner has the reduce signature, and its output must have the same type as a map
output value, because the reducer will see a mix of combined and uncombined values.
"""

import re

_WORD = re.compile(r"[a-z]+")


# ---- word count (§2.1, Appendix A) ------------------------------------------------
def wc_map(offset, line):
    for word in _WORD.findall(line.lower()):
        yield word, 1


def wc_reduce(word, counts):
    return sum(counts)              # associative + commutative -> safe as a combiner


# ---- average temperature per city: where a combiner goes WRONG -----------------------
def temp_map(offset, line):
    city, temp = line.split(",")
    yield city, float(temp)


def mean_reduce(city, temps):
    return round(sum(temps) / len(temps), 4)
# Using mean_reduce as a combiner computes a mean of per-split means. That is not
# the overall mean unless every split holds the same number of readings per city.


# The fix: carry (sum, count), which is associative, and divide only at the very end.
def temp_map_pair(offset, line):
    city, temp = line.split(",")
    yield city, [float(temp), 1]


def sum_pair_combine(city, pairs):
    return [sum(p[0] for p in pairs), sum(p[1] for p in pairs)]


def mean_from_pairs(city, pairs):
    total, count = sum_pair_combine(city, pairs)
    return round(total / count, 4)


# ---- custom partitioner (§4.1) -------------------------------------------------------
def first_letter_partition(key, R):
    """Keeps every word with the same first letter in one output file."""
    return (ord(key[0]) - ord("a")) % R
