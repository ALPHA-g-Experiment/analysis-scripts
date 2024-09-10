#!/usr/bin/env python3

import argparse
import math
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import polars as pl

parser = argparse.ArgumentParser(
    description="Visualize the reconstructed annihilation vertices for a single run",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument("vertices_csv", help="path to the reconstructed vertices CSV file")
parser.add_argument("--output", help="write output to `OUTPUT`")
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
    pl.read_csv(args.vertices_csv, comment_prefix="#")
    .with_columns(
        phi=pl.arctan2("reconstructed_y", "reconstructed_x"),
        r=(pl.col("reconstructed_x").pow(2) + pl.col("reconstructed_y").pow(2)).sqrt(),
    )
    .filter(
        pl.col("trg_time").is_between(args.t_min, args.t_max),
        pl.col("reconstructed_z").is_between(args.z_min, args.z_max),
        pl.col("phi").is_between(args.phi_min, args.phi_max),
        pl.col("r").is_between(args.r_min, args.r_max),
    )
)
num_vertices = len(df)

fig = plt.figure(figsize=(19, 10), dpi=100)

ax = fig.add_subplot(231)
z_edges, z_bin_width = np.linspace(
    args.z_min, args.z_max, args.z_bins + 1, retstep=True
)
ax.hist(df["reconstructed_z"], bins=z_edges)
ax.set(xlabel="z [m]", ylabel="Number of vertices")

ax = fig.add_subplot(232)
t_max = args.t_max if args.t_max < float("inf") else df["trg_time"].max()
t_edges, t_bin_width = np.linspace(args.t_min, t_max, args.t_bins + 1, retstep=True)
ax.hist(df["trg_time"], bins=t_edges)
ax.set(xlabel="TRG time [s]", ylabel="Number of vertices")

ax = fig.add_subplot(233)
_, _, _, mesh = ax.hist2d(
    df["trg_time"], df["reconstructed_z"], bins=[t_edges, z_edges], cmin=1
)
ax.set(xlabel="TRG time [s]", ylabel="z [m]")
cbar = fig.colorbar(mesh)
cbar.set_label("Number of vertices", rotation=270, labelpad=15)

ax = fig.add_subplot(234)
r_edges, r_bin_width = np.linspace(
    args.r_min, args.r_max, args.r_bins + 1, retstep=True
)
hist, _, _ = ax.hist(df["r"], bins=r_edges)
ax.set(xlabel="r [m]", ylabel="Number of vertices")
ax = ax.twinx()
ax.set(yticklabels=[])
norm = hist / (math.pi * (r_edges[1:] ** 2 - r_edges[:-1] ** 2))
ax.hist(r_edges[:-1], r_edges, weights=norm, histtype="step", color="tab:orange")
ax.legend(
    handles=[
        matplotlib.lines.Line2D([], [], c="tab:orange", label="Radial density [a.u.]")
    ]
)

ax = fig.add_subplot(235)
phi_edges, phi_bin_width = np.linspace(
    args.phi_min, args.phi_max, args.phi_bins + 1, retstep=True
)
ax.hist(df["phi"], bins=phi_edges)
ax.set(xlabel="phi [rad]", ylabel="Number of vertices")

axc = fig.add_subplot(236)
axc.set(xlabel="x [m]", ylabel="y [m]")
axc.set_aspect("equal")
hist, _, _ = np.histogram2d(df["phi"], df["r"], bins=[phi_edges, r_edges])
hist[hist < 1] = np.nan
axc.set_xlim(-r_edges[-1], r_edges[-1])
axc.set_ylim(-r_edges[-1], r_edges[-1])
ax = fig.add_subplot(236, projection="polar")
ax.set(xticklabels=[], yticklabels=[])
ax.grid(False)
X, Y = np.meshgrid(phi_edges, r_edges)
pc = ax.pcolormesh(X, Y, hist.T)
cbar = fig.colorbar(pc, ax=[ax, axc], location="right")
cbar.set_label("Number of vertices", rotation=270, labelpad=15)

text = "\n".join(
    [
        r"$\bf{Bin\ widths:}$",
        r"$\Delta z$: {:.2E} m".format(z_bin_width),
        r"$\Delta t$: {:.2E} s".format(t_bin_width),
        r"$\Delta r$: {:.2E} m".format(r_bin_width),
        r"$\Delta \phi$: {:.2E} rad".format(phi_bin_width),
        "",
        r"$\bf{Number\ of\ vertices:}$" + f" {num_vertices}",
    ]
)
fig.text(0.005, 0.01, text)

if args.output:
    plt.savefig(args.output, bbox_inches="tight")
else:
    plt.show()
