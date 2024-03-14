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
