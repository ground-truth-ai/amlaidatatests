from typing import List

import pytest

from amlaidatatests.cli import entry_point


def test_skipped_test(capsys, protect_config):
    # Test P035 is for an optional table (partysupplementary data)
    with pytest.raises(SystemExit) as excinfo:
        entry_point(["--connection_string=duckdb://", "-k P035"])
    #
    assert excinfo.value.code == 0
    out, _ = capsys.readouterr()
    # This is the last line in the output - the next is a blank line
    final_line = out.split("\n")[-2]
    assert "1 skipped" in final_line


def test_failed_test(capsys, protect_config):
    # Check for party.type which doesn't exist
    with pytest.raises(SystemExit) as excinfo:
        entry_point(["--connection_string=duckdb://", "-k E001"])
    assert excinfo.value.code == 1
    out, _ = capsys.readouterr()
    # This is the last line
    lines: List[str] = out.split("\n")
    line = None
    for i, line in enumerate(lines):
        if "failures: 1" in line:
            # Whitespace is important here
            assert (
                lines[i + 1]
                == "E001   party.type                         "
                + "                   ValueError: Required table party does not exist\t"
            )
        break
    # Catch where either no lines processed, or nothing matched failure
    if not line or line == len(lines):
        raise Exception("Could not find a line indicating failures in test output")
