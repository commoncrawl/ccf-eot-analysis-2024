import sys

import duckdb

import utils


utils.set_memory_limit_gbytes(128)


eot2024 = duckdb.read_parquet('eot2024_hostlevel_logs.parquet')

sq = sys.argv[1]
duckdb.sql(sq).show(max_width=1000, max_rows=30)
