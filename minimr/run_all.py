"""Run all three demos and write results/RESULTS.md plus the charts in results/."""

import os
import platform

import demo_backup
import demo_combiner
import demo_fault

if __name__ == "__main__":
    os.makedirs("results", exist_ok=True)
    print("Demo 1: combiner ...", flush=True)
    part1 = demo_combiner.report(demo_combiner.wordcount(), demo_combiner.average())
    print("Demo 2: backup tasks ...", flush=True)
    part2 = demo_backup.report(*demo_backup.run())
    print("Demo 3: worker failure ...", flush=True)
    part3 = demo_fault.report(*demo_fault.run())

    header = (f"# minimr results\n\nMachine: {platform.platform()}, {os.cpu_count()} CPU cores, "
              f"Python {platform.python_version()}. Timings depend on the machine; ratios are what matter.\n\n")
    text = header + "\n".join([part1, part2, part3])
    with open("results/RESULTS.md", "w", encoding="utf-8") as f:
        f.write(text)
    print(text)
