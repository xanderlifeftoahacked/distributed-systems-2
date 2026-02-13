import csv
import pathlib

import matplotlib.pyplot as plt

root = pathlib.Path(__file__).resolve().parent
csv_path = root / "results.csv"
rows = []
with csv_path.open() as f:
    reader = csv.DictReader(f)
    for row in reader:
        rows.append(row)

if not rows:
    raise SystemExit("results.csv is empty")

def to_float(row, key):
    return float(row[key])

def mean(values):
    return sum(values) / len(values)

def by_mode_rf(put_ratio, metric):
    data = [r for r in rows if float(r["putRatio"]) == put_ratio and r["replicationMode"] in {"sync", "async"}]
    data.sort(key=lambda r: int(r["rf"]))
    rfs = sorted(set(int(r["rf"]) for r in data))
    means = {"sync": [], "async": []}
    points = {"sync": {}, "async": {}}
    for mode in means:
        for rf in rfs:
            vals = [to_float(r, metric) for r in data if r["replicationMode"] == mode and int(r["rf"]) == rf]
            means[mode].append(mean(vals))
            points[mode][rf] = vals
    return rfs, means, points

for ratio in [0.8, 0.2]:
    rfs, means, points = by_mode_rf(ratio, "throughputOpsSec")
    plt.figure()
    for mode, values in means.items():
        plt.plot(rfs, values, marker="o", label=mode)
        for rf in rfs:
            xs = [rf] * len(points[mode][rf])
            plt.scatter(xs, points[mode][rf], alpha=0.4)
    plt.title(f"throughput vs RF (putRatio={ratio})")
    plt.xlabel("RF")
    plt.ylabel("ops/sec")
    plt.legend()
    plt.savefig(root / f"throughput_rf_{ratio}.png")

for ratio in [0.8, 0.2]:
    rfs, means, points = by_mode_rf(ratio, "p95Ms")
    plt.figure()
    for mode, values in means.items():
        plt.plot(rfs, values, marker="o", label=mode)
        for rf in rfs:
            xs = [rf] * len(points[mode][rf])
            plt.scatter(xs, points[mode][rf], alpha=0.4)
    plt.title(f"p95 latency vs RF (putRatio={ratio})")
    plt.xlabel("RF")
    plt.ylabel("p95 ms")
    plt.legend()
    plt.savefig(root / f"p95_rf_{ratio}.png")

for ratio in [0.8, 0.2]:
    data = [r for r in rows if float(r["putRatio"]) == ratio and int(r["rf"]) == 3 and r["replicationMode"] in {"async", "semi-sync", "sync"}]
    modes_order = ["async", "semi-sync", "sync"]
    means = []
    for mode in modes_order:
        vals = [to_float(r, "throughputOpsSec") for r in data if r["replicationMode"] == mode]
        means.append(mean(vals))
    plt.figure()
    plt.bar(modes_order, means)
    plt.title(f"throughput at RF=3 (putRatio={ratio})")
    plt.xlabel("mode")
    plt.ylabel("ops/sec")
    plt.savefig(root / f"throughput_modes_{ratio}.png")
