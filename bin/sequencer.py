#!/usr/bin/env python3

from typing import NamedTuple
import argparse
import json
import polars as pl
import xml.etree.ElementTree as ET


def sequencer_name(xml_string: str) -> str:
    root = ET.fromstring(xml_string)
    # Setting default to "" allows us to handle missing elements and missing
    # text the same way.
    sequencer_name = root.findtext("SequencerName", default="")
    if sequencer_name == "":
        raise ValueError("error finding sequencer name in XML")
    return sequencer_name


class SequencerEvent(NamedTuple):
    name: str
    description: str


def event_table(xml_string: str) -> list[SequencerEvent]:
    root = ET.fromstring(xml_string)
    events = []
    for event in root.iter("event"):
        # Setting default to "" allows us to handle missing elements and missing
        # text the same way.
        name = event.findtext("name", default="")
        description = event.findtext("description", default="")
        if name == "" or description == "":
            raise ValueError("error finding event table in XML")
        else:
            events.append(SequencerEvent(name, description))
    return events


parser = argparse.ArgumentParser(
    description="Extract sequencer events information for a single run.",
    formatter_class=argparse.RawDescriptionHelpFormatter,
)
parser.add_argument("sequencer_csv", help="path to the sequencer CSV file")
group = parser.add_argument_group(
    "advanced",
    """Find the Chronobox timestamp of all sequencer events.
Write output as CSV with the following columns:
sequencer_name,event_name,event_description,chronobox_time""",
)
group.add_argument("--odb-json", help="path to the ODB JSON file")
group.add_argument("--chronobox-csv", help="path to the Chronobox CSV file")
args = parser.parse_args()
if bool(args.odb_json) ^ bool(args.chronobox_csv):
    parser.error("--odb-json and --chronobox-csv must be used together")

sequencer_df = pl.read_csv(args.sequencer_csv, comment_prefix="#").select(
    "midas_timestamp",
    sequencer_name=pl.col("xml").map_elements(sequencer_name),
    event_table=pl.col("xml").map_elements(event_table),
)

if args.odb_json is None and args.chronobox_csv is None:

    def pretty_string(events) -> str:
        dumps = []
        for event in events:
            description = event["description"].strip('"')
            name = event["name"]
            if name == "startDump":
                dumps.append("Start " + description)
            elif name == "stopDump" and dumps and dumps[-1] == "Start " + description:
                dumps[-1] = description
            else:
                dumps.append(description)
        return "\n".join(dumps)

    sequencer_df = sequencer_df.select(
        "midas_timestamp",
        "sequencer_name",
        pl.col("event_table").map_elements(pretty_string),
    )
    with pl.Config(
        fmt_str_lengths=2**15 - 1,
        tbl_formatting="ASCII_HORIZONTAL_ONLY",
        tbl_hide_column_data_types=True,
        tbl_hide_dataframe_shape=True,
        tbl_rows=-1,
    ):
        print(sequencer_df)
else:

    def sequence_running_channel_name(sequencer_name: str) -> str:
        return sequencer_name.upper() + "_SEQ_RUNNING"

    def start_dump_channel_name(sequencer_name: str) -> str:
        return sequencer_name.upper() + "_START_DUMP"

    def stop_dump_channel_name(sequencer_name: str) -> str:
        return sequencer_name.upper() + "_STOP_DUMP"

    class ChronoboxChannel(NamedTuple):
        board: str
        channel: int

    def chronobox_channel(odb: dict, channel_name: str) -> ChronoboxChannel:
        known_boards = ["cb01", "cb02", "cb03", "cb04"]
        found_channels = []
        for board in known_boards:
            names = odb["Equipment"][board]["Settings"]["names"]
            for channel, name in enumerate(names):
                if name == channel_name:
                    found_channels.append(ChronoboxChannel(board, channel))

        if len(found_channels) == 1:
            return found_channels[0]
        else:
            raise ValueError(f"error finding {channel_name} in ODB")

    chronobox_df = (
        pl.read_csv(args.chronobox_csv, comment_prefix="#")
        .filter(
            pl.col("leading_edge"),
        )
        .select("board", "channel", "chronobox_time")
    )
    # First 2 lines are comments
    json_string = open(args.odb_json).read().split("\n", 2)[2]
    odb = json.loads(json_string)

    result = pl.DataFrame()
    for (name,), seq_df in sequencer_df.group_by(["sequencer_name"]):
        seq_running = chronobox_channel(odb, sequence_running_channel_name(name))
        start_dump = chronobox_channel(odb, start_dump_channel_name(name))
        stop_dump = chronobox_channel(odb, stop_dump_channel_name(name))

        seq_df = seq_df.sort("midas_timestamp")
        cb_df = chronobox_df.filter(
            (
                pl.col("board").eq(seq_running.board)
                & pl.col("channel").eq(seq_running.channel)
            )
            | (
                pl.col("board").eq(start_dump.board)
                & pl.col("channel").eq(start_dump.channel)
            )
            | (
                pl.col("board").eq(stop_dump.board)
                & pl.col("channel").eq(stop_dump.channel)
            )
        ).sort("chronobox_time")
        if cb_df["chronobox_time"].null_count() > 0:
            raise ValueError(f"found null chronobox time for `{name}`")

        seq_df = (
            seq_df.with_row_index()
            .join(
                cb_df.with_row_index("running_index")
                .filter(board=seq_running.board, channel=seq_running.channel)
                .with_columns(
                    next_running_index=pl.col("running_index").shift(
                        -1, fill_value=cb_df.select(pl.len())
                    )
                )
                .with_row_index(),
                on="index",
                how="left",
            )
            .with_columns(
                difference=pl.col("midas_timestamp") - pl.col("chronobox_time"),
            )
            .select(
                "sequencer_name",
                "running_index",
                "next_running_index",
                "event_table",
                difference=(pl.col("difference") - pl.first("difference")).abs(),
            )
        )
        # Allow some difference between the MIDAS timestamp of the sequencer
        # event and the Chronobox "SEQUENCE_RUNNING" hit.
        if seq_df.filter(pl.col("difference") > 5).select(pl.len()).item() > 0:
            raise ValueError(f"found large MIDAS timestamp difference for `{name}`")

        cb_df = (
            cb_df.with_row_index()
            .filter(
                (pl.col("board") != seq_running.board)
                | (pl.col("channel") != seq_running.channel)
            )
            .join(
                seq_df.explode("event_table").with_columns(
                    index=(
                        (
                            pl.col("running_index")
                            + pl.col("running_index").cum_count()
                        ).clip(upper_bound=pl.col("next_running_index"))
                    ).over("running_index")
                ),
                on="index",
                how="left",
            )
            .unnest("event_table")
            .select(
                "sequencer_name",
                pl.col("name").alias("event_name"),
                pl.col("description").alias("event_description"),
                "chronobox_time",
                "board",
                "channel",
            )
        )
        if cb_df["event_name"].null_count() > 0:
            raise ValueError(f"found extra Chronobox hits for `{name}`")
        if (
            cb_df.filter(board=start_dump.board, channel=start_dump.channel)
            .select(pl.col("event_name").eq("startDump").all().not_())
            .item()
        ) or (
            cb_df.filter(board=stop_dump.board, channel=stop_dump.channel)
            .select(pl.col("event_name").eq("stopDump").all().not_())
            .item()
        ):
            raise ValueError(f"found event and Chronobox mismatch for `{name}`")

        cb_df = cb_df.select(
            "sequencer_name",
            "event_name",
            pl.col("event_description").str.strip_chars('"'),
            "chronobox_time",
        )
        result.vstack(cb_df, in_place=True)

    result = result.sort("chronobox_time")
    print(result.write_csv())
