import argparse
import numpy as np
import polars as pl

parser = argparse.ArgumentParser(
    description="Visualize the TRG scalers for a single run.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument("infile", help="path to the TRG scalers CSV file")
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

df = pl.read_csv(args.infile, comment_prefix="#").filter(
    pl.col("trg_time").is_between(args.t_min, args.t_max)
)
# All histograms should have the same binning
_, t_edges = np.histogram(df["trg_time"], bins=args.t_bins)

for name, included in columns.items():
    if not included:
        continue
    """
    We only know the time of the output counters. For all the other ones we just
    know by how much they were incremented. The best we can do is assume that
    those counts are evenly spread out over the time interval.
    """
    col_df = (
        df.filter(pl.col(name).is_not_null())
        .rename({"trg_time": "t_right"})
        .with_columns(
            (pl.col("t_right") - pl.col("t_right").diff()).alias("t_left"),
            pl.col(name).diff().alias("counts"),
        )
        .filter(pl.col("counts") > 0)
        .select(["t_left", "t_right", "counts"])
    )

    times = []
    for t_left, t_right, counts in col_df.rows():
        times.extend(np.linspace(t_left, t_right, counts + 1)[1:])
