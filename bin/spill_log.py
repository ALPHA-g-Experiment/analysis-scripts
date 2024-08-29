#!/usr/bin/env python3

import argparse
import json
import polars as pl

parser = argparse.ArgumentParser(
    description="Generate the spill log.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
# This takes the output of the `sequencer.py` script NOT the output of the
# `alpha-g-sequencer` core binary.
parser.add_argument(
    "sequencer_events_csv",
    help="path to the sequencer events CSV file (produced by sequencer.py)",
)
parser.add_argument("odb_json", help="path to the ODB JSON file")
parser.add_argument("chronobox_csv", help="path to the Chronobox timestamps CSV file")
parser.add_argument("--output", help="write output to `OUTPUT`")
args = parser.parse_args()

windows_df = (
    pl.read_csv(args.sequencer_events_csv)
    .sort("chronobox_time")
    .group_by(["sequencer_name", "event_description"])
    .agg(
        start_time=pl.col("chronobox_time").filter(pl.col("event_name") == "startDump"),
        stop_time=pl.col("chronobox_time").filter(pl.col("event_name") == "stopDump"),
    )
    .explode(["start_time", "stop_time"])
)

# First 2 lines are comments
json_string = open(args.odb_json).read().split("\n", 2)[2]
odb = json.loads(json_string)

chronobox_df = (
    pl.read_csv(args.chronobox_csv, comment_prefix="#")
    .with_columns(
        channel_name=pl.struct("board", "channel").map_elements(
            lambda x: odb["Equipment"][x["board"]]["Settings"]["names"][x["channel"]],
            return_dtype=pl.String,
        )
    )
    .filter(
        pl.col("leading_edge"),
        # Ignore all channels that have duplicate names in the ODB just because
        # it makes my life easier. The only really annoying thing would be to
        # name the columns in the spill log for these duplicates, but it's just
        # easier to make a habit of using unique names in the ODB.
        pl.struct("board", "channel").n_unique().over("channel_name") == 1,
    )
    .select("channel_name", "chronobox_time")
)

# The following gymnastics can be greatly simplified by non-equi joins, but this
# approach scales better with the number of chronobox events and multiple levels
# of nested windows (doesn't require to `explode` events in every window).
chronobox_df = (
    chronobox_df.drop_nulls().sort("channel_name", "chronobox_time").with_row_index()
)
spill_log_df = (
    windows_df.join(chronobox_df.select(pl.col("channel_name").unique()), how="cross")
    .sort("channel_name", "start_time")
    .join_asof(
        chronobox_df,
        left_on="start_time",
        right_on="chronobox_time",
        strategy="forward",
        by="channel_name",
    )
    .drop("chronobox_time")
    .rename({"index": "first_index"})
    .sort("channel_name", "stop_time")
    .join_asof(
        chronobox_df,
        left_on="stop_time",
        right_on="chronobox_time",
        strategy="backward",
        by="channel_name",
    )
    .drop("chronobox_time")
    .rename({"index": "last_index"})
    .with_columns(counts=pl.col("last_index") + 1 - pl.col("first_index"))
    .pivot(
        on="channel_name",
        index=["sequencer_name", "event_description", "start_time", "stop_time"],
        values="counts",
        sort_columns=True,
    )
    .fill_null(0)
    .sort("start_time", "stop_time")
)

if args.output:
    spill_log_df.write_csv(args.output)
else:
    print(spill_log_df.write_csv())
