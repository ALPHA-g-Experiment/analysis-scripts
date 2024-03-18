#!/usr/bin/env python3

from typing import NamedTuple
import argparse
import polars as pl
import xml.etree.ElementTree as ET


def sequencer_name(xml_string: str) -> str:
    root = ET.fromstring(xml_string)
    # Setting default to "" allows us to handle missing elements and missing
    # text the same way.
    sequencer_name = root.findtext("SequencerName", default="")
    if sequencer_name == "":
        raise ValueError("Invalid sequencer XML")
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
            raise ValueError("Invalid sequencer XML")
        else:
            events.append(SequencerEvent(name, description))
    return events


def pretty_string(events: list[SequencerEvent]) -> str:
    dumps = []
    for event in events:
        description = event.description.strip('"')
        if event.name == "startDump":
            dumps.append("Start " + description)
        elif event.name == "stopDump" and dumps and dumps[-1] == "Start " + description:
            dumps[-1] = description
        else:
            dumps.append(description)
    return "\n".join(dumps)


parser = argparse.ArgumentParser(
    description="Extract sequencer events information for a single run."
)
parser.add_argument("sequencer_csv", help="path to the sequencer CSV file")
args = parser.parse_args()

sequencer_df = pl.read_csv(args.sequencer_csv, comment_prefix="#").select(
    "midas_timestamp",
    sequencer_name=pl.col("xml").map_elements(sequencer_name),
    event_table=pl.col("xml").map_elements(event_table, return_dtype=pl.Object),
)

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
