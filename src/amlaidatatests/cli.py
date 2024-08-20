"""Utility CLI for amlaidatatests"""

import argparse
import sys
import typing
from typing import List

from amlaidatatests.config import ConfigSingleton, init_parser_options_from_config
from amlaidatatests.connection import connection_factory
from amlaidatatests.schema.utils import get_amlai_schema, get_table_name
from amlaidatatests.tests import run_tests


def create_skeleton(args):
    cfg = ConfigSingleton.get()
    version = cfg.schema_version
    schema = get_amlai_schema(version)
    connection = connection_factory()
    for table in schema.TABLES:
        connection.create_table(name=get_table_name(table.name), schema=table.schema)


def build_parser():
    parser = argparse.ArgumentParser()
    parser = init_parser_options_from_config(parser)
    parser = typing.cast(argparse.ArgumentParser, parser)
    parser.add_argument(
        "--pytest-help",
        help="Print extra additional flags from pytest and exit",
        dest="pytesthelp",
        action="store_true",
    )
    parser.add_argument(
        "--show-sql",
        help="Show SQL for errors/warnings",
        dest="showsql",
        action="store_true",
    )
    return parser


def entry_point(sysargs: List[str] | None = None) -> None:
    """Configure an argparse based entry point for amlaidatatests

    This is configured somewhat differently from the pytest dataset, which is
    somewhat clunky to configure"""
    parser = build_parser()

    if sysargs is None:
        sysargs = sys.argv[1:]

    if "--pytest-help" in sysargs:
        run_tests(["-h"])
        # will exit

    args, extra = parser.parse_known_args(sysargs)

    if args.pytesthelp:
        sysargs.remove("--pytest-help")

    # show sql really just shows the full tracebacks which
    # prints the full sql
    show_sql = ["--tb=no", "--disable-warnings"]
    if args.showsql:
        show_sql = ["--tb=short"]
        sysargs.remove("--show-sql")

    sysargs += show_sql

    # For now, just pass all command line arguments through. This allows us to
    # verify the arguments but then pass them through directly -c NONE prevents
    # pytest from discovering setup.cfg elsewhere in the filesytem. this
    # prevents it printing a relative path to the root directory
    run_tests(
        ["-W ignore::DeprecationWarning", "-c NONE", "-rN", "--no-header", *sysargs]
    )


if __name__ == "__main__":
    entry_point()
