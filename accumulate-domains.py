import sys
import os.path

import pyarrow as pa
import pyarrow.parquet as pq
import smart_open

import utils


class Domains:
    def __init__(self, fname, host_parquet_columns):
        self.registered_domains = {}
        self.fname = os.path.splitext(os.path.basename(fname))[0]
        self.host_columns = host_parquet_columns

        self.host_row_copies = [
            'url_host_registered_domain',
            'url_host_tld',
            'fed_csv_registered_domain',
        ]
        host_row_ignores = [
            'hcrank', 'prank', 'surt_host_name', 'url_host_name', 'robots_code', 'robots_duration',
        ]
        self.host_row_adds = set(host_parquet_columns).difference(set(self.host_row_copies), set(host_row_ignores))

    def init_domain(self, url_host_registered_domain, parquet_row):
        self.registered_domains[url_host_registered_domain] = {
            'url_host_registered_domain': url_host_registered_domain,
        }
        entry = self.registered_domains[url_host_registered_domain]
        for key in self.host_row_copies:
            entry[key] = parquet_row[key]
        for key in self.host_row_adds:
            entry[key] = 0

        # we assume SURT order, so all the subdomains are together
        self.subdomains = set()
        self.subdomains_with_2xx = set()
        entry['n_subdomains'] = 0
        entry['n_subdomains_with_2xx'] = 0

    def accumulate(self, parquet_row):
        r_d = parquet_row['url_host_registered_domain']
        if r_d not in self.registered_domains:
            self.init_domain(r_d, parquet_row)
        entry = self.registered_domains[r_d]
        self.subdomains.add(parquet_row['surt_host_name'])
        entry['n_subdomains'] = len(self.subdomains)
        for key in self.host_row_adds:
            try:
                entry[key] += int(parquet_row[key].as_py())
            except Exception as e:
                print('about to crash on key', key)
                raise
        if int(parquet_row['codes_2xx'].as_py()) > 0:
            self.subdomains_with_2xx.add(parquet_row['surt_host_name'])
            entry['n_subdomains'] = len(self.subdomains_with_2xx)

    def spill(self):
        table = pa.Table.from_pylist(list(self.registered_domains.values()))
        fname = f'{self.fname}.parquet'
        pq.write_table(table, fname)


def main():
    for fname in sys.argv[1:2]:
        host_parquet_columns = utils.parquet_columns(fname)
        domains = Domains('eot2024_domainlevel_logs', host_parquet_columns)
        for row in utils.iter_parquet_rows(fname):
            row_d = dict(zip(host_parquet_columns, row))
            domains.accumulate(row_d)
    domains.spill()


if __name__ == '__main__':
    utils.set_memory_limit_gbytes(128)
    main()

