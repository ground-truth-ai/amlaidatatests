#!/usr/bin/env python

import pathlib
import re
import xml.etree.ElementTree as ET

path = pathlib.Path(__file__).parent


if __name__ == "__main__":
    # Given a region.xml from the unicode CLDR spec (https://cldr.unicode.org/index/cldr-spec)
    # common/validity/region.xml
    tree = ET.parse(source=path.joinpath("region.xml"))
    root = tree.getroot()
    child = root.findall("./idValidity/id[@idStatus='regular'][1]")[0]
    with open(path.joinpath("country_codes.csv"), "w", encoding="utf-8") as f:
        f.write("code\n")
        for s in re.split(r"\s+", child.text):
            if s == "":
                continue
            # A range of characters
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
