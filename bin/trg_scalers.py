import argparse
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

df = pl.read_csv(args.infile, comment_prefix="#").filter(
    pl.col("trg_time").is_between(args.t_min, args.t_max)
)
