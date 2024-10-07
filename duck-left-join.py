import sys

import duckdb

import utils


utils.set_memory_limit_gbytes(128)


left, right, output = sys.argv[1:4]

left_cols = set(utils.parquet_columns(left))
#right_cols = set(utils.parquet_columns(right))
right_cols = ('hcrank', 'prank', 'captures')

left_db = duckdb.read_parquet(left, hive_partitioning=True)
right_db = duckdb.read_parquet(right, hive_partitioning=True)

# left: surt_host_name, right: url_host_name_reversed

# expected overlap: captures
inter = left_cols.intersection(right_cols)
assert inter == set(['captures'])

union = left_cols.union(right_cols)
#union.remove('url_host_name_reversed')
#union.add('right_db.hcrank')

# fix up this right-side column to not be a NULL in the left join
fixup = 'hcrank'
try:
    union.remove(fixup)
except KeyError:
    raise ValueError(f'could not find column "{fixup}" in the database')
union.add(f'COALESCE({fixup}, 0) AS {fixup}')

# fix up this right-side column to not be a NULL in the left join
fixup = 'captures'
try:
    union.remove(fixup)
except KeyError:
    raise ValueError(f'could not find column "{fixup}" in the database')
union.add('left_db.captures')
union.add(f'COALESCE(right_db.{fixup}, 0) AS ccf_{fixup}')  # note the ccf_

cols_str = ', '.join(union)

on = 'ON left_db.surt_host_name = right_db.url_host_name_reversed'
select = f'SELECT {cols_str} FROM left_db LEFT OUTER JOIN right_db {on}'

sq = f"COPY ({select}) TO '{output}' (FORMAT 'parquet');"

duckdb.sql(sq)
