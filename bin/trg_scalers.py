#!/usr/bin/env python3

import argparse
from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
import numpy as np
import polars as pl

parser = argparse.ArgumentParser(
    description="Visualize the TRG scalers for a single run.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument("trg_scalers_csv", help="path to the TRG scalers CSV file")
parser.add_argument("--output", help="write output to `OUTPUT`")
parser.add_argument("--t-bins", type=int, default=100, help="number of bins along t")
parser.add_argument(
    "--t-max", type=float, default=float("inf"), help="maximum time in seconds"
)
parser.add_argument("--t-min", type=float, default=0.0, help="minimum time in seconds")
parser.add_argument("--include-drift-veto-counter", action="store_true")
parser.add_argument("--include-pulser-counter", action="store_true")
parser.add_argument("--include-scaledown-counter", action="store_true")
parser.add_argument("--remove-input-counter", action="store_true")
parser.add_argument("--remove-output-counter", action="store_true")
args = parser.parse_args()

columns = {
    "input": not args.remove_input_counter,
    "drift_veto": args.include_drift_veto_counter,
    "scaledown": args.include_scaledown_counter,
    "pulser": args.include_pulser_counter,
    "output": not args.remove_output_counter,
}

df = pl.read_csv(args.trg_scalers_csv, comment_prefix="#").filter(
    pl.col("trg_time").is_between(args.t_min, args.t_max)
)

t_max = args.t_max if args.t_max < float("inf") else df["trg_time"].max()
t_edges, t_bin_width = np.linspace(args.t_min, t_max, args.t_bins + 1, retstep=True)
text = r"$\bf{Bin\ width:}$" + f" {t_bin_width:.2E} s"
plt.figtext(0.005, 0.01, text, fontsize=8)

for name, included in columns.items():
    if not included:
        continue
    """
    We only know the time of the output counters. For all the other ones we just
    know by how much they were incremented. The best we can do is assume that
    those counts are evenly spread out over the time interval.
    """
    times = np.array(
        df.filter(pl.col(name).is_not_null())
        .rename({"trg_time": "t_right"})
        .with_columns(
            t_left=pl.col("t_right") - pl.col("t_right").diff(),
            counts=pl.col(name).diff(),
        )
        .filter(pl.col("counts") > 0)
        .select(
            "t_left",
            step=((pl.col("t_right") - pl.col("t_left")) / pl.col("counts")),
            i=pl.int_ranges(1, pl.col("counts") + 1),
        )
        .explode("i")
        .select(times=pl.col("t_left") + pl.col("step") * pl.col("i"))
    )
    if df[name][0] > 0:
        times = np.append(df["trg_time"][0], times)

    plt.hist(
        times, bins=t_edges, histtype="step", label=f"{name} ({times.size} counts)"
    )

plt.xlabel("TRG time [s]")
plt.ylabel("Counts")

handles, labels = plt.gca().get_legend_handles_labels()
new_handles = [Line2D([], [], c=h.get_edgecolor()) for h in handles]
plt.legend(handles=new_handles, labels=labels)
plt.tight_layout()

if args.output:
    plt.savefig(args.output)
else:
    plt.show()
