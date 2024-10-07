import resource

import pyarrow.parquet as pq


def iter_parquet_rows(file, wanted=None):
    table = pq.ParquetFile(file)
    for group_i in range(table.num_row_groups):
        row_group = table.read_row_group(group_i)

        for batch in row_group.to_batches():
            if wanted:
                indexes = [batch.column_names.index(w) for w in wanted]
            else:
                indexes = range(batch.num_columns)
            for row in zip(*[batch.column(i) for i in indexes]):
                yield row


def iter_parquet_batches(file, wanted=None):
    if wanted is not None:
        # can use RecordBatch.select([cols]) to implement this
        raise NotImplementedError('sorry not implemented')

    table = pq.ParquetFile(file)
    for group_i in range(table.num_row_groups):
        row_group = table.read_row_group(group_i)

        for batch in row_group.to_batches():
            yield batch


def set_memory_limit_gbytes(gbytes):
    # this limit is in bytes. Linux only.
    bytes_ = int(gbytes * 1024*1024*1024)
    _, hard = resource.getrlimit(resource.RLIMIT_AS)
    resource.setrlimit(resource.RLIMIT_AS, (bytes_, hard))


def parquet_columns(file):
    return pq.ParquetFile(file).schema.names
