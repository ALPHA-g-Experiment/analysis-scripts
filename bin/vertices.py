import argparse
import math
import matplotlib.pyplot as plt
import polars as pl

parser = argparse.ArgumentParser(
    description="Visualize the reconstructed annihilation vertices for a single run",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument("infile", help="path to the reconstructed vertices CSV file")
"""
All default thresholds represent the detector dimensions.
Some events are definitely reconstructed outside these thresholds, but we
most likely just want to ignore them.
"""
parser.add_argument(
    "--max-phi", type=float, default=math.pi, help="maximum azimuthal angle in radians"
)
parser.add_argument(
    "--max-r", type=float, default=0.19, help="maximum radial coordinate in meters"
)
parser.add_argument(
    "--max-t", type=float, default=float("inf"), help="maximum time in seconds"
)
parser.add_argument(
    "--max-z", type=float, default=1.152, help="maximum z coordinate in meters"
)
parser.add_argument(
    "--min-phi", type=float, default=-math.pi, help="minimum azimuthal angle in radians"
)
parser.add_argument(
    "--min-r", type=float, default=0.0, help="minimum radial coordinate in meters"
)
parser.add_argument("--min-t", type=float, default=0.0, help="minimum time in seconds")
parser.add_argument(
    "--min-z", type=float, default=-1.152, help="minimum z coordinate in meters"
)
parser.add_argument(
    "--phi-bins", type=int, default=100, help="number of bins along phi"
)
parser.add_argument("--r-bins", type=int, default=100, help="number of bins along r")
parser.add_argument("--t-bins", type=int, default=100, help="number of bins along t")
parser.add_argument("--z-bins", type=int, default=100, help="number of bins along z")
args = parser.parse_args()

df = (
    pl.read_csv(args.infile, comment_prefix="#")
    .with_columns(
        pl.arctan2("reconstructed_y", "reconstructed_x").alias("phi"),
        (pl.col("reconstructed_x").pow(2) + pl.col("reconstructed_y").pow(2))
        .sqrt()
        .alias("r"),
    )
    .filter(
        pl.col("trg_time").is_between(args.min_t, args.max_t),
        pl.col("reconstructed_z").is_between(args.min_z, args.max_z),
        pl.col("phi").is_between(args.min_phi, args.max_phi),
        pl.col("r").is_between(args.min_r, args.max_r),
    )
)

fig, axs = plt.subplots(2, 3)
axs[0, 0].hist(df["reconstructed_z"], bins=args.z_bins)
axs[0, 0].set(xlabel="z [m]", ylabel="Number of vertices")

axs[0, 1].hist(df["trg_time"], bins=args.t_bins)
axs[0, 1].set(xlabel="TRG time [s]", ylabel="Number of vertices")

axs[0, 2].hist2d(
    df["trg_time"], df["reconstructed_z"], bins=[args.t_bins, args.z_bins], cmin=1
)
axs[0, 2].set(xlabel="TRG time [s]", ylabel="z [m]")

axs[1, 0].hist(df["r"], bins=args.r_bins)
axs[1, 0].set(xlabel="r [m]", ylabel="Number of vertices")

axs[1, 1].hist(df["phi"], bins=args.phi_bins)
axs[1, 1].set(xlabel="phi [rad]", ylabel="Number of vertices")

axs[1, 2].hist2d(df["reconstructed_x"], df["reconstructed_y"], bins=100, cmin=1)

plt.show()
