#!/usr/bin/env python3

from typing import NamedTuple, Optional
import argparse
import json
import polars as pl
import sys
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
            # https://github.com/pola-rs/polars/issues/15425
            events.append(SequencerEvent(name, description)._asdict())
    return events


parser = argparse.ArgumentParser(
    description="Extract sequencer events information for a single run.",
    formatter_class=argparse.RawDescriptionHelpFormatter,
)
parser.add_argument("sequencer_csv", help="path to the sequencer CSV file")
parser.add_argument("--output", help="write output to `OUTPUT`")
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
    sequencer_name=pl.col("xml").map_elements(sequencer_name, return_dtype=pl.String),
    event_table=pl.col("xml").map_elements(
        event_table,
        return_dtype=pl.List(pl.Struct({"name": pl.String, "description": pl.String})),
    ),
)

if args.odb_json is None and args.chronobox_csv is None:

    def pretty_string(events) -> str:
        dumps = []
        for event in events:
            description = event["description"].strip('"')
            name = event["name"]
            if name == "startDump":
                dumps.append("Start " + description)
            elif name == "stopDump":
                if dumps and dumps[-1] == "Start " + description:
                    dumps[-1] = description
                else:
                    dumps.append("Stop " + description)
            else:
                raise ValueError(f"unknown event `{name} ({description})`")
        return "\n".join(dumps)

    sequencer_df = sequencer_df.select(
        "midas_timestamp",
        "sequencer_name",
        pl.col("event_table").map_elements(pretty_string, return_dtype=pl.String),
    )
    with pl.Config(
        fmt_str_lengths=2**15 - 1,
        tbl_formatting="ASCII_HORIZONTAL_ONLY",
        tbl_hide_column_data_types=True,
        tbl_hide_dataframe_shape=True,
        tbl_rows=-1,
    ):
        if args.output:
            with open(args.output, "w") as f:
                f.write(str(sequencer_df))
        else:
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

    def chronobox_channel(odb: dict, channel_name: str) -> Optional[ChronoboxChannel]:
        known_boards = ["cb01", "cb02", "cb03", "cb04"]
        found_channels = []
        for board in known_boards:
            names = odb["Equipment"][board]["Settings"]["names"]
            for channel, name in enumerate(names):
                if name == channel_name:
                    found_channels.append(ChronoboxChannel(board, channel))

        if len(found_channels) == 1:
            # https://github.com/pola-rs/polars/issues/15425
            return found_channels[0]._asdict()
        elif len(found_channels) == 0:
            return None
        else:
            raise ValueError(f"multiple `{channel_name}` channels in ODB")

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
    # The sequencer XMLs are reliable to let us know if a sequence started
    # running, but its timestamp is only good to within a few seconds. On the
    # other hand, the Chronobox timestamps are good, but it has some noise/false
    # positives (we detect leading edges, and it looks like the sequencer can't
    # always keep the "SEQ_RUNNING" signal high, so it falls and rises again
    # every now and then).
    # Hence we need to match the sequencer XMLs to the Chronobox "SEQ_RUNNING"
    # timestamps (filtering out false positives).
    sequencer_df = sequencer_df.with_columns(
        cb_start=pl.col("sequencer_name")
        .map_elements(
            lambda x: chronobox_channel(odb, start_dump_channel_name(x)),
            return_dtype=pl.Struct({"board": pl.String, "channel": pl.Int64}),
            skip_nulls=False,
        )
        .name.suffix_fields("_start"),
        cb_stop=pl.col("sequencer_name")
        .map_elements(
            lambda x: chronobox_channel(odb, stop_dump_channel_name(x)),
            return_dtype=pl.Struct({"board": pl.String, "channel": pl.Int64}),
            skip_nulls=False,
        )
        .name.suffix_fields("_stop"),
        cb_running=pl.col("sequencer_name")
        .map_elements(
            lambda x: chronobox_channel(odb, sequence_running_channel_name(x)),
            return_dtype=pl.Struct({"board": pl.String, "channel": pl.Int64}),
            skip_nulls=False,
        )
        .name.suffix_fields("_running"),
    )
    # Some times people randomly run A2 sequencers (e.g. atm, rct, etc) to do
    # stuff like a random MCP dump. These sequencer signals are usually not
    # connected to the Chronoboxes, so instead of crashing, we just ignore them.
    # This doesn't affect at all the other sequences.
    for (name,) in (
        sequencer_df.filter(pl.any_horizontal(pl.all().is_null()))
        .select("sequencer_name")
        .unique()
        .rows()
    ):
        print(
            f"Ignoring `{name}` sequencer (chronobox channels not found in ODB).",
            file=sys.stderr,
        )
    sequencer_df = sequencer_df.drop_nulls().unnest("cb_start", "cb_stop", "cb_running")

    matched_seq_running = False
    cb_running_df = chronobox_df.join(
        sequencer_df,
        left_on=["board", "channel"],
        right_on=["board_running", "channel_running"],
        how="semi",
    )
    for (shift,) in (
        sequencer_df.select(
            pl.first("midas_timestamp", "board_running", "channel_running")
        )
        .join(
            cb_running_df,
            left_on=["board_running", "channel_running"],
            right_on=["board", "channel"],
            how="inner",
        )
        .select(shift=pl.col("midas_timestamp") - pl.col("chronobox_time").round())
        .rows()
    ):
        temp = (
            sequencer_df.with_columns(
                shifted_timestamp=pl.col("midas_timestamp") - shift
            )
            .sort("shifted_timestamp")
            .join_asof(
                cb_running_df.sort("chronobox_time"),
                left_on="shifted_timestamp",
                right_on="chronobox_time",
                by_left=["board_running", "channel_running"],
                by_right=["board", "channel"],
                strategy="nearest",
                tolerance=2.0,
            )
            .rename({"chronobox_time": "start_time"})
        )

        if temp.filter(pl.col("start_time").is_null()).height == 0:
            matched_seq_running = True
            sequencer_df = temp
            break
    if not matched_seq_running:
        # This failure means that we couldn't match all sequencer XMLs to a
        # "SEQ_RUNNING" hit in a Chronobox. To debug this, the easiest would be
        # to print the `temp` DataFrame above for all attempted `shifts` and see
        # what's going on. The most likely causes are:
        # 1. The tolerance is too low. Just increase it. This is expected, the
        #    XML timestamps are not very accurate.
        # 2. The "SEQ_RUNNING" hit for an XML is missing in the Chronobox data.
        #    Find out why and fix it. Maybe the cable is not connected.
        #    To fix this for a run that has already been taken, just add a fake
        #    Chronobox hit in the `chronobox_timestamps.csv` file by hand.
        raise ValueError("failed to match `SEQ_RUNNING` signals")

    sequencer_df = sequencer_df.with_columns(
        next_start_time=pl.col("start_time")
        .shift(-1, fill_value=float("inf"))
        .over("sequencer_name"),
    )

    expected_df = (
        sequencer_df.explode("event_table")
        .unnest("event_table")
        .select(
            "sequencer_name",
            "start_time",
            event_name="name",
            event_description=pl.col("description").str.strip_chars('"'),
            index=pl.int_range(pl.len()).over("sequencer_name", "start_time"),
        )
    )

    observed_df = (
        pl.concat(
            [
                sequencer_df.join_where(
                    chronobox_df,
                    pl.col("chronobox_time") >= pl.col("start_time"),
                    pl.col("chronobox_time") < pl.col("next_start_time"),
                    pl.col("board") == pl.col("board_start"),
                    pl.col("channel") == pl.col("channel_start"),
                ).with_columns(event_name=pl.lit("startDump")),
                sequencer_df.join_where(
                    chronobox_df,
                    pl.col("chronobox_time") >= pl.col("start_time"),
                    pl.col("chronobox_time") < pl.col("next_start_time"),
                    pl.col("board") == pl.col("board_stop"),
                    pl.col("channel") == pl.col("channel_stop"),
                ).with_columns(event_name=pl.lit("stopDump")),
            ]
        )
        .select(
            "sequencer_name",
            "start_time",
            "event_name",
            "chronobox_time",
        )
        .sort("chronobox_time", maintain_order=True)
        .with_columns(index=pl.int_range(pl.len()).over("sequencer_name", "start_time"))
    )

    result = (
        observed_df.join(
            expected_df,
            on=["sequencer_name", "start_time", "event_name", "index"],
            how="left",
        )
        .with_columns(
            pl.when(pl.col("event_description").is_null().cum_sum() == 0)
            .then("event_description")
            .over("sequencer_name", "start_time")
        )
        .select("sequencer_name", "event_name", "event_description", "chronobox_time")
    )
    for (name,) in (
        result.filter(pl.col("event_description").is_null())
        .select("sequencer_name")
        .unique()
        .rows()
    ):
        print(
            f"Ignoring mismatched dump markers for `{name}` sequencer.",
            file=sys.stderr,
        )
    result = result.drop_nulls()

    if args.output:
        result.write_csv(args.output)
    else:
        print(result.write_csv())
