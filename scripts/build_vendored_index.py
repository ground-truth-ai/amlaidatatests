#!/usr/bin/env python

# Quick script to build a vendored index for packaged visualization

import csv
import sys
from urllib.parse import urlparse

argument = sys.argv[1]

data = []

with open(argument) as f:
    a = f.read().splitlines()
    for l in a:
        url = urlparse(l)
        if url.scheme != "gs":
            raise Exception("All lines in this file must start with gs")
        # print url.
        path_split = url.path.split("/")

        file = path_split[-1]
        link = f"`{file} <https://storage.googleapis.com/{url.netloc}{url.path}>`__"
        # Ignore paths which are just the folder
        if len(path_split) == 3 and file != "":
            data.append({"version": path_split[-2], "url": link})

# Sort by version descending
data.sort(key=lambda x: str([x["version"], x["url"]]), reverse=True)
fieldnames = ["version", "url"]

with open("index.csv", "w", newline="") as csvfile:
    spamwriter = csv.writer(
        csvfile, delimiter=" ", quotechar='"', quoting=csv.QUOTE_MINIMAL
    )
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data)
