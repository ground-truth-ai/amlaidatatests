import ibis

# TODO: Suffix needs to be configurable (probably via Omegaconf)
SUFFIX = "1234"

connection = ibis.connect("duckdb://duckdb.ddb")

