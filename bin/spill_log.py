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
# Use the TRG scalers to give an approximate number of input trigger counters.
# Given the frequency of the TRG  output counter, this is a good enough
# approximation. An exact count would need the chronobox_csv output to include
# the cbtrg, which it currently doesn't (and probably never will because it
# makes the CSV files huge and it's not really necessary).
parser.add_argument("trg_scalers_csv", help="path to the TRG scalers CSV file")
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

trg_scalers_df = pl.read_csv(
    args.trg_scalers_csv,
    comment_prefix="#",
    # Schema is necessary because this CSV can be empty (and that should still
    # be a valid spill log, just with TRG counters set to 0).
    schema={
        "serial_number": pl.Int64,
        "trg_time": pl.Float64,
        "input": pl.Int64,
        "drift_veto": pl.Int64,
        "scaledown": pl.Int64,
        "pulser": pl.Int64,
        "output": pl.Int64,
    },
)

# The following gymnastics can be greatly simplified by non-equi joins, but this
# approach scales better with the number of chronobox events and multiple levels
# of nested windows (doesn't require to `explode` events in every window).
chronobox_df = (
    chronobox_df.drop_nulls().sort("channel_name", "chronobox_time").with_row_index()
)
cb_spill_log_df = (
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
)

trg_scalers_df.sort("trg_time")
trg_spill_log_df = (
    windows_df.sort("start_time")
    .join_asof(
        trg_scalers_df,
        left_on="start_time",
        right_on="trg_time",
        strategy="forward",
    )
    .drop("serial_number", "trg_time", "drift_veto", "scaledown", "pulser", "output")
    .rename({"input": "first_input"})
    .sort("stop_time")
    .join_asof(
        trg_scalers_df,
        left_on="stop_time",
        right_on="trg_time",
        strategy="backward",
    )
    .drop("serial_number", "trg_time", "drift_veto", "scaledown", "pulser", "output")
    .rename({"input": "last_input"})
    .with_columns(
        trg_approx_input=(pl.col("last_input") - pl.col("first_input")).clip(0)
    )
    .drop("first_input", "last_input")
)

spill_log_df = (
    cb_spill_log_df.join(
        trg_spill_log_df,
        on=["sequencer_name", "event_description", "start_time", "stop_time"],
        how="left",
    )
    .fill_null(0)
    .sort("start_time", "stop_time")
)

if args.output:
    spill_log_df.write_csv(args.output)
else:
    print(spill_log_df.write_csv())
