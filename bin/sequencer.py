#!/usr/bin/env python3

from typing import NamedTuple
import xml.etree.ElementTree as ET


class Event(NamedTuple):
    """
    Sequencer event.
    """

    name: str
    description: str


def sequencer_name(xml_string: str) -> str:
    """
    Extract the name of a sequencer from its XML string.

        Parameters:
            xml_string: The XML string to parse.

        Returns:
            sequencer_namer: The name of the sequencer.

        Raises:
            ValueError: If the input string is not valid sequencer XML.
    """
    root = ET.fromstring(xml_string)
    # Setting default to "" allows us to handle missing elements and missing
    # text the same way.
    sequencer_name = root.findtext("SequencerName", default="")
    if sequencer_name == "":
        raise ValueError("Invalid sequencer XML")

    return sequencer_name


def event_table(xml_string: str) -> list[Event]:
    """
    Extract all events from a sequencer XML string.

        Parameters:
            xml_string: The XML string to parse.

        Returns:
            events: Ordered list of all the events in the sequence.

        Raises:
            ValueError: If the input string is not valid sequencer XML.
    """
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
            events.append(Event(name, description))

    return events


if __name__ == "__main__":
    import argparse
    import polars as pl

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

    def pretty_string(events: list[Event]) -> str:
        dumps = []
        for event in events:
            description = event.description.strip('"')
            if event.name == "startDump":
                dumps.append("Start " + description)
            elif (
                event.name == "stopDump"
                and dumps
                and dumps[-1] == "Start " + description
            ):
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
