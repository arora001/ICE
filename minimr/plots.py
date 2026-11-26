"""Charts for the demos (matplotlib, PNG)."""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker
from matplotlib.patches import Patch

INK, INK2, GRID, AXIS = "#191919", "#666666", "#e7e7e7", "#c2c2c2"
BLUE, ORANGE, RED = "#257de0", "#f47230", "#ec4647"     # categorical slots 1, 2 and 8

plt.rcParams.update({
    "font.size": 10, "text.color": INK, "axes.labelcolor": INK2,
    "axes.edgecolor": AXIS, "xtick.color": INK2, "ytick.color": INK2,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
    "axes.axisbelow": True, "figure.dpi": 150, "savefig.bbox": "tight",
})


def combiner_bars(off, on, path):
    """Two small panels (records, MB) rather than one chart with two y-axes."""
    fig, axes = plt.subplots(1, 2, figsize=(8, 3.4))
    panels = [("Intermediate records", off["intermediate_records"], on["intermediate_records"], 1),
              ("Intermediate data (MB)", off["intermediate_bytes"] / 1e6, on["intermediate_bytes"] / 1e6, 1)]
    for ax, (title, a, b, _) in zip(axes, panels):
        bars = ax.bar(["Combiner off", "Combiner on"], [a, b], color=[AXIS, BLUE], width=0.55)
        ax.set_title(title, loc="left", color=INK, fontsize=10)
        ax.grid(axis="x", visible=False)
        for bar, v in zip(bars, [a, b]):
            label = f"{v:,.0f}" if v >= 100 else f"{v:.1f}"
            ax.annotate(label, (bar.get_x() + bar.get_width() / 2, v), ha="center",
                        va="bottom", xytext=(0, 3), textcoords="offset points", color=INK)
        ax.set_ylim(0, a * 1.15)
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(
            lambda v, _: f"{v / 1e6:.1f}M" if v >= 1e5 else f"{v:,.0f}"))
    fig.suptitle(f"Combiner cuts the shuffled data by {1 - on['intermediate_records'] / off['intermediate_records']:.0%}"
                 " (word count, 2M words)", x=0.02, ha="left", fontsize=11, color=INK)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def _gantt(ax, result, n_workers, title, horizon):
    for rec in result.timeline:
        colour = BLUE if rec["kind"] == "map" else ORANGE
        y, x0, w = rec["worker"], rec["start"], max(rec["end"] - rec["start"], 0.004)
        if rec["outcome"] == "committed":
            ax.barh(y, w, left=x0, height=0.62, color=colour, edgecolor="white", linewidth=1)
        elif rec["outcome"] == "lost (worker died)":
            ax.barh(y, w, left=x0, height=0.62, color="white", edgecolor=RED, linewidth=1.2, hatch="xx")
        else:   # duplicate (ignored) / abandoned / failed: work that was thrown away
            ax.barh(y, w, left=x0, height=0.62, color="white", edgecolor=colour, linewidth=1.2, hatch="///")
        if w > horizon * 0.028:
            ax.text(x0 + w / 2, y, f"{rec['kind'][0]}{rec['task']}", ha="center", va="center",
                    fontsize=7, color="white" if rec["outcome"] == "committed" else INK)
    ax.axvline(result.elapsed, color=INK, linewidth=1, linestyle=(0, (3, 2)))
    ax.annotate(f"job done {result.elapsed:.2f}s", (result.elapsed, n_workers - 0.45),
                xytext=(4, 0), textcoords="offset points", fontsize=8, color=INK, va="top")
    ax.set_yticks(range(n_workers), [f"worker {w}" for w in range(n_workers)])
    ax.set_ylim(n_workers - 0.5, -0.5)
    ax.grid(axis="y", visible=False)
    ax.set_title(title, loc="left", fontsize=10, color=INK)


_LEGEND = [Patch(facecolor=BLUE, label="map (committed)"),
           Patch(facecolor=ORANGE, label="reduce (committed)"),
           Patch(facecolor="white", edgecolor=INK2, hatch="///", label="thrown away (duplicate / abandoned)"),
           Patch(facecolor="white", edgecolor=RED, hatch="xx", label="lost when worker died")]


def gantt_pair(top, bottom, titles, n_workers, path, suptitle):
    horizon = max(top.elapsed, bottom.elapsed,
                  *(r["end"] for r in top.timeline + bottom.timeline)) * 1.08
    fig, axes = plt.subplots(2, 1, figsize=(9, 5.2), sharex=True)
    for ax, res, title in zip(axes, (top, bottom), titles):
        _gantt(ax, res, n_workers, title, horizon)
        ax.set_xlim(0, horizon)
    axes[1].set_xlabel("seconds since job start")
    fig.legend(handles=_LEGEND, loc="lower center", ncol=4, frameon=False, fontsize=8,
               bbox_to_anchor=(0.5, -0.04))
    fig.suptitle(suptitle, x=0.02, ha="left", fontsize=11, color=INK)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(path)
    plt.close(fig)


def gantt_single(result, n_workers, path, title):
    fig, ax = plt.subplots(figsize=(9, 2.9))
    horizon = max(result.elapsed, *(r["end"] for r in result.timeline)) * 1.12
    _gantt(ax, result, n_workers, "", horizon)
    ax.set_xlim(0, horizon)
    ax.set_xlabel("seconds since job start")
    fig.legend(handles=_LEGEND, loc="lower center", ncol=4, frameon=False, fontsize=8,
               bbox_to_anchor=(0.5, -0.1))
    fig.suptitle(title, x=0.02, ha="left", fontsize=11, color=INK)
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    fig.savefig(path)
    plt.close(fig)


def backup_times(off_times, on_times, path):
    import statistics
    fig, ax = plt.subplots(figsize=(6, 3.2))
    meds = [statistics.median(off_times), statistics.median(on_times)]
    labels = ["Backup tasks off", "Backup tasks on"]
    bars = ax.barh(labels, meds, color=[AXIS, BLUE], height=0.5)
    for i, times in enumerate((off_times, on_times)):
        ax.scatter(times, [i] * len(times), color=INK, s=12, zorder=3, label="single run" if i == 0 else None)
    for bar, m, times in zip(bars, meds, (off_times, on_times)):
        ax.annotate(f"median {m:.2f}s", (max(times), bar.get_y() + bar.get_height() / 2), xytext=(8, 0),
                    textcoords="offset points", va="center", color=INK, fontsize=9)
    ax.set_xlim(0, max(off_times) * 1.4)
    ax.invert_yaxis()
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("job completion time (s), one worker 10x slower")
    ax.legend(frameon=False, fontsize=8, loc="upper right")
    slower = meds[0] / meds[1] - 1
    ax.set_title(f"Without backups the job takes {slower:.0%} longer", loc="left", fontsize=11, color=INK)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
