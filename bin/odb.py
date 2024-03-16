#!/usr/bin/env python3

from jsonpointer import resolve_pointer
import argparse
import json

parser = argparse.ArgumentParser(description="Extract information from the ODB")
parser.add_argument("odb_json", help="path to the ODB JSON file")
parser.add_argument(
    "json_pointer",
    help="JSON pointer to resolve (e.g. /Equipment/cbtrg/Settings/names/0)",
)
parser.add_argument("--pretty", action="store_true", help="pretty print the output")
args = parser.parse_args()

with open(args.odb_json) as f:
    # First 2 lines are comments
    json_string = f.read().split("\n", 2)[2]
    odb = json.loads(json_string)

    result = resolve_pointer(odb, args.json_pointer, None)
    if args.pretty:
        print(json.dumps(result, indent=4))
    else:
        print(result)
