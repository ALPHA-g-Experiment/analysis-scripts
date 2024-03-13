import argparse
from collections import namedtuple
import polars as pl
import xml.etree.ElementTree as ET

parser = argparse.ArgumentParser(
    description="Extract the chronobox and TRG time of all sequencer events for a single run"
)
parser.add_argument("seguencer", help="path to the sequencer CSV file")
parser.add_argument("chronobox", help="path to the Chronobox timestamps CSV file")
args = parser.parse_args()

Event = namedtuple("Event", ["name", "description"])


def sequencer_name(xml_string):
    """
    Extract the name of a sequence from its XML string.

        Parameters:
            xml_string (str): The XML string to parse.

        Returns:
            sequencer_name (str): The name of the sequence.
    """
    root = ET.fromstring(xml_string)
    return root.find("SequencerName").text


def event_table(xml_string):
    """
    Extract all events from a sequencer XML string.

        Parameters:
            xml_string (str): The XML string to parse.

        Returns:
            events (list): Ordered list of all the events in the sequence.
    """
    root = ET.fromstring(xml_string)
    events = []
    for event in root.iter("event"):
        name = event.find("name").text
        description = event.find("description").text
        events.append(Event(name, description))

    return events


sequencer_df = pl.read_csv(args.seguencer, comment_prefix="#").select(
    "midas_timestamp",
    name=pl.col("xml").map_elements(sequencer_name),
    events=pl.col("xml").map_elements(event_table),
)
