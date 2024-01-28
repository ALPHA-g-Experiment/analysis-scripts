import argparse
import math
import matplotlib.pyplot as plt
import numpy as np
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
    "--phi-bins", type=int, default=100, help="number of bins along phi"
)
parser.add_argument(
    "--phi-max", type=float, default=math.pi, help="maximum azimuthal angle in radians"
)
parser.add_argument(
    "--phi-min", type=float, default=-math.pi, help="minimum azimuthal angle in radians"
)
parser.add_argument("--r-bins", type=int, default=100, help="number of bins along r")
parser.add_argument(
    "--r-max", type=float, default=0.19, help="maximum radial coordinate in meters"
)
parser.add_argument(
    "--r-min", type=float, default=0.0, help="minimum radial coordinate in meters"
)
parser.add_argument("--t-bins", type=int, default=100, help="number of bins along t")
parser.add_argument(
    "--t-max", type=float, default=float("inf"), help="maximum time in seconds"
)
parser.add_argument("--t-min", type=float, default=0.0, help="minimum time in seconds")
parser.add_argument("--z-bins", type=int, default=100, help="number of bins along z")
parser.add_argument(
    "--z-max", type=float, default=1.152, help="maximum z coordinate in meters"
)
parser.add_argument(
    "--z-min", type=float, default=-1.152, help="minimum z coordinate in meters"
)
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
        pl.col("trg_time").is_between(args.t_min, args.t_max),
        pl.col("reconstructed_z").is_between(args.z_min, args.z_max),
        pl.col("phi").is_between(args.phi_min, args.phi_max),
        pl.col("r").is_between(args.r_min, args.r_max),
    )
)

fig = plt.figure()

ax = plt.subplot(231)
ax.set(xlabel="z [m]", ylabel="Number of vertices")
ax.hist(df["reconstructed_z"], bins=args.z_bins)

ax = plt.subplot(232)
ax.set(xlabel="TRG time [s]", ylabel="Number of vertices")
ax.hist(df["trg_time"], bins=args.t_bins)

ax = plt.subplot(233)
ax.set(xlabel="TRG time [s]", ylabel="z [m]")
h = ax.hist2d(
    df["trg_time"], df["reconstructed_z"], bins=[args.t_bins, args.z_bins], cmin=1
)
cbar = fig.colorbar(h[3])
cbar.set_label("Number of vertices", rotation=270, labelpad=15)

ax = plt.subplot(234)
ax.set(xlabel="r [m]", ylabel="Number of vertices")
ax.hist(df["r"], bins=args.r_bins)

ax = plt.subplot(235)
ax.set(xlabel="phi [rad]", ylabel="Number of vertices")
ax.hist(df["phi"], bins=args.phi_bins)

axc = plt.subplot(236)
axc.set(xlabel="x [m]", ylabel="y [m]")
axc.set_aspect("equal")
hist, phi_edges, r_edges = np.histogram2d(
    df["phi"], df["r"], bins=[args.phi_bins, args.r_bins]
)
hist[hist < 1] = np.nan
axc.set_xlim(-r_edges[-1], r_edges[-1])
axc.set_ylim(-r_edges[-1], r_edges[-1])
ax = plt.subplot(236, projection="polar")
ax.set(xticklabels=[], yticklabels=[])
ax.grid(False)
X, Y = np.meshgrid(phi_edges, r_edges)
pc = ax.pcolormesh(X, Y, hist.T)
cbar = fig.colorbar(pc, ax=[ax, axc], location="right")
cbar.set_label("Number of vertices", rotation=270, labelpad=15)

plt.show()
