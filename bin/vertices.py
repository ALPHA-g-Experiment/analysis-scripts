import argparse
import polars as pl
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(
    description="Visualize the reconstructed annihilation vertices for a single run",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument("infile", help="path to the reconstructed vertices CSV file")
parser.add_argument(
    "--min-time",
    type=float,
    default=0.0,
    help="minimum time in seconds",
)
parser.add_argument(
    "--max-time",
    type=float,
    default=float("inf"),
    help="maximum time in seconds",
)
parser.add_argument(
    "--min-z",
    type=float,
    default=float("-inf"),
    help="minimum z-coordinate in meters",
)
parser.add_argument(
    "--max-z",
    type=float,
    default=float("inf"),
    help="maximum z-coordinate in meters",
)
parser.add_argument(
    "--z-bins",
    type=int,
    default=100,
    help="number of bins along z",
)
args = parser.parse_args()

df = pl.read_csv(args.infile, comment_prefix="#").filter(
    pl.col("trg_time").is_between(args.min_time, args.max_time),
    pl.col("reconstructed_z").is_between(args.min_z, args.max_z),
)

plt.hist(df.select("reconstructed_z"), bins=args.z_bins)
plt.xlabel("z [m]")
plt.ylabel("Counts")
plt.show()
