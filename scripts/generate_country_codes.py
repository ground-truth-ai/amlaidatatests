#!/usr/bin/env python

import importlib.resources
import os
import pathlib
import re
import xml.etree.ElementTree as ET

path = pathlib.Path(__file__).parent

THIS_DIRECTORY = os.path.dirname(__file__)


def generate_country_codes(cldr_region_xml: pathlib.Path, output_path: pathlib.Path):
    """Given a region.xml from the unicode CLDR spec
    (https://cldr.unicode.org/index/cldr-spec), generate a CSV input file
    specifying the country codes"""
    tree = ET.parse(source=cldr_region_xml)
    root = tree.getroot()
    child = root.findall("./idValidity/id[@idStatus='regular'][1]")[0]
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("code\n")
        for s in re.split(r"\s+", child.text):
            if s == "":
                continue
            # A tilde indicates a range of characters
            # AA~G -> AA AB AC AD AE AF AG
            if ("~" in s) and len(s) == 4:
                ranges = s.split("~")
                start_range, end_range = ranges
                static_letter = start_range[0]
                letter_start_range = ord(start_range[1])
                letter_end_range = ord(end_range[0])
                for letter in range(letter_start_range, letter_end_range + 1):
                    f.write(f"{static_letter}{chr(letter)}\n")
                continue
            if len(s) == 2:
                f.write(f"{s}\n")
                continue
            raise ValueError(f"Unexpected input {s}")


if __name__ == "__main__":
    # Update the region.xml file to the csv format to our lookup format
    base_directory = importlib.resources.files("amlaidatatests.resources")

    generate_country_codes(
        cldr_region_xml=base_directory.joinpath("region.xml"),
        output_path=base_directory.joinpath("country_codes.csv"),
    )
