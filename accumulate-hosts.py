import sys
from collections import defaultdict
import os.path
import urllib.parse
import csv

#from HLL import HyperLogLog
import tldextract
import pyarrow as pa
import pyarrow.parquet as pq
import smart_open

import utils


def parse_netloc(netloc):
    # people say the Internet is simple, and it is... for them.
    # is it still true that the python urllib can't parse a netloc? how is that possible
    if '@' in netloc:
        userpassword, _, netloc = netloc.partition('@')
        if ':' in userpassword:
            user, password = userpassword.split(':', 1)
        else:
            user = userpassword
            password = ''
    else:
        user = ''
        password = ''
    if ':' in netloc:
        if (('[' in netloc and ']' not in netloc or
             ']' in netloc and '[' not in netloc)):
            # invalid ipv6 address. don't try to get a port
            hostname = netloc
            port = ''
        elif '[' in netloc:
            # valid ipv6 address
            if netloc.endswith(']'):
                hostname = netloc
                port = ''
            else:
                hostname, _, port = netloc.rpartition(':')
        else:
            hostname, _, port = netloc.rpartition(':')
    else:
        hostname = netloc
        port = ''
    return user, password, hostname, port


def split_hostname(parts):
    _, _, hostname, _ = parse_netloc(parts.netloc)
    tld = hostname.split('.')[-1]

    try:
        tlde = tldextract.extract(hostname, include_psl_private_domains=False)
    except IndexError:
        # can be raised for punycoded hostnames
        raise
    rd = tlde.registered_domain
    if not rd:
        # I don't have a current example of this, but it's happened in the past
        rd = tlde.suffix
    registered_domain = rd

    return hostname, registered_domain, tld


def make_surt_host_name(uri, parts=None):
    if not parts:
        parts = urllib.parse.urlsplit(uri)
    url_host_name, url_host_registered_domain, url_host_tld = split_hostname(parts)

    if url_host_name == url_host_registered_domain:
        pass
    elif url_host_name.endswith('.'+url_host_registered_domain):
        pass
    else:
        # example: ip addr
        #raise ValueError(f'surprised by host {url_host_name} registered domain {url_host_registered_domain}')
        pass

    if url_host_name.startswith('www.') and url_host_name != url_host_registered_domain:
        url_host_name = url_host_name[4:]  # drop the www
    surt_host_name = '.'.join(reversed(url_host_name.split('.')))
    return surt_host_name, url_host_name, url_host_registered_domain, url_host_tld


def update_code(entry, code):
    # given a code, increment the correct fields in entry
    # https://heritrix.readthedocs.io/en/latest/glossary.html#status-codes

    entry['captures'] += 1

    debug = entry.copy()

    if code == '666':
        entry['codes_666'] += 1
    elif code == '999':
        entry['codes_999'] += 1
    elif len(code) == 3 and code.isdigit():
        field = 'codes_' + code[0:1] + 'xx'
        if field not in entry:
            print(f'surprised by 3 digit code {code} field {field}')
            entry['codes_xxx'] += 1
        else:
            entry[field] += 1
    elif code == '-404':
        entry['codes_minus_404'] += 1
    elif code == '-9998':
        entry['codes_robots_disallowed'] += 1
    elif code == '-61':
        entry['codes_robots_fetch_failed'] += 1
    elif code in {'-2', '-3', '-4'}:
        entry['codes_network_failed']
    elif code == '-8':
        entry['codes_retries_exhausted'] += 1
    elif code == '0':
        entry['codes_0'] += 1
    elif code.startswith('-'):
        entry['codes_other_minus'] += 1
    else:
        print(f'surprised by code {code}')

    # compare debug to now
    for key in debug:
        if debug[key] != entry[key]:
            if not key.endswith('xx') and code != '999' and code != '-6':
                print(code, key)


class Hosts:
    def __init__(self, fname, fed_csv_registered_domains=set()):
        self.surt_host_names = {}
        self.fname = os.path.splitext(os.path.basename(fname))[0]
        self.fed_csv_registered_domains = fed_csv_registered_domains
        self.part = 0

    def init_host(self, surt_host_name, url_host_name, url_host_registered_domain, url_host_tld):
        self.surt_host_names[surt_host_name] = {
            'surt_host_name': surt_host_name,
            #'distinct_urls': HyperLogLog(p=4),  # 128 bytes

            'captures': 0,  # includes 200s
            'codes_2xx': 0,
            'codes_3xx': 0,
            'codes_4xx': 0,
            'codes_5xx': 0,
            'codes_666': 0,  # non-standard
            'codes_999': 0,  # non-standard
            'codes_0': 0,  # "fetch never tried", 3219
            'codes_other_minus': 0,  # 51k
            'codes_xxx': 0,  # 3 digit int but not a valid one, 1

            'codes_minus_4xx': 0,  # 0, but a bunch of robots_code are -404
            'codes_robots_disallowed': 0,  # XXX always zero?
            'codes_robots_fetch_failed': 0,  # XXX always zero?
            'codes_network_failed': 0,  # XXX always zero

            'codes': defaultdict(int),
            'durations': defaultdict(int),
            #'hcrank': None,  # will come from join
            'url_host_name': url_host_name,
            'url_host_registered_domain': url_host_registered_domain,
            'url_host_tld': url_host_tld,
            'robots_code': None,
            'robots_duration': None,
            'fed_csv_registered_domain': url_host_registered_domain in self.fed_csv_registered_domains,
        }

    def accumulate(self, uri, code, timestamp_duration):
        if uri.startswith('dns:'):
            # perhaps record the duration ?
            return

        parts = urllib.parse.urlsplit(uri)
        surt_host_name, url_host_name, url_host_registered_domain, url_host_tld = make_surt_host_name(uri, parts=parts)

        if surt_host_name not in self.surt_host_names:
            self.init_host(surt_host_name, url_host_name, url_host_registered_domain, url_host_tld)
        entry = self.surt_host_names[surt_host_name]

        if parts.path == '/robots.txt':
            # likely only 1 of these per host, after previous redirs
            # just remember the last one
            entry['robots_code'] = code
            if '+' not in timestamp_duration:
                if code == '-404' and timestamp_duration == '-':
                    pass
                else:
                    print(f'weird: robots code {code} t_d {timestamp_duration}')
            else:
                entry['robots_duration'] = timestamp_duration.split('+')[1]
            return

        #entry['distinct_urls'].add(uri)
        update_code(entry, code)

        if '+' not in timestamp_duration:
            if uri.startswith('ftp:'):
                pass
            elif code.startswith('-') and timestamp_duration == '-':
                pass
            else:
                print(f'weird uri {uri} code {code} t_d {timestamp_duration}')
            duration = '0'
        else:
            duration = timestamp_duration.split('+')[1]
        # to shrink the size of this dict, round to 100ms
        d = str(round(int(duration) / 100.) * 100)
        entry['durations'][d] += 1

        # spill if the in-memory db is too big
        if len(self.surt_host_names) > 1_000_000:
            self.spill()

    def spill(self):
        # build a pyarrow table, then output to parquet

        # non-dicts, HLLs, dicts are separate
        ordinary = []
        #hlls = []
        codes_d = []
        durations_d = []
        for k, v in self.surt_host_names.items():
            #hlls.append({'surt_host_name': v['surt_host_name'], 'distinct_urls': v['distinct_urls']})
            codes_d.append({'surt_host_name': v['surt_host_name'], 'codes': v['codes']})
            durations_d.append({'surt_host_name': v['surt_host_name'], 'durations': v['durations']})

            entry = v.copy()
            for field in ('codes', 'durations'):  # 'distinct_urls',
                del entry[field]
            ordinary.append(entry)

        table = pa.Table.from_pylist(ordinary)
        fname = f'{self.fname}_{self.part:05}.parquet'
        pq.write_table(table, fname)

        # XXX
        #table = pa.Table.from_pylist(hlls)
        #fname = f'{self.fname}_hlls_{self.part:05}.parquet'
        #pq.write_table(table, fname)

        # the dicts need to be expanded before writing
        # XXX

        self.part += 1


def main():
    fed_csv_registered_domains = set()
    with open('current-federal.csv', newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            fed_csv_registered_domains.add(row[0])

    for fname in sys.argv[1:]:
        hosts = Hosts('first_log_drop_debug', fed_csv_registered_domains=fed_csv_registered_domains)
        with smart_open.open(fname) as fd:
            for line in fd:
                # see logtrix CrawlLogIterator.java
                parts = line.split(maxsplit=12)
                len_parts = len(parts)
                if len_parts < 10:
                    print(f'surprised by short log line of length {len_parts}, skipping')
                    continue

                #timestamp = parts[0]
                code = parts[1]  # https://heritrix.readthedocs.io/en/latest/glossary.html#status-codes
                #size = parts[2]
                uri = parts[3]
                #hop_path = parts[4]  # P, PR, PL
                #parent_url = parts[5]
                #mimetype = parts[6]
                #toe_thread_number = parts[7]  # ignore, #021
                timestamp_duration = parts[8]
                #digest = parts[9]  # sha1:...

                source_tags = None  # -
                annotation = None  # -
                if len(parts) > 10:
                    source_tags = parts[10]  # ignore. apparently allowed to be missing?
                    if source_tags != '-':
                        print(f'saw source_tags {uri} {source_tags}')
                if len(parts) > 11:
                    annotation = parts[11]  # allowed to have spaces, allowed to be missing
                    if annotation != '-':
                        # example: https://ice.disa.mil/images/ICE2016v2grad2.svg 3t
                        #print(f'saw annotation tag {uri} {annotation}')
                        pass

                hosts.accumulate(uri, code, timestamp_duration)
    hosts.spill()

'''
howto store a list in parquet

pyarrow struct: pa.struct(list) can be stored
'''

'''
howto store a dict in parquet
1. make a json string
     duckdb: https://duckdb.org/docs/extensions/json#json-extraction-functions
     JSON Extraction Functions
     apache arrow: https://docs.aws.amazon.com/athena/latest/ug/querying-JSON.html
     no this is just how to turn jsonl into a sql table
     https://docs.aws.amazon.com/athena/latest/ug/extracting-data-from-JSON.html
     also no
2. pyarrow.struct
     this is strict and you have to give it all key names in advance
3. use a separate table with 1 row per dict value
     this is trival to sum in sql
'''

        
'''

hll = HyperLogLog(p=4)  # 2^4 x 64 bits
hll.add(str)
hll.merge(hll2)
hll.cardinality()

'''
        
'''
host rollup
unique urls -- hyperloglog?
dict codes
latency histogram ?
  just 50%, 95, 99
  round to 10ms and RLE?
  that looks like a dict
hcrank
url_host_registered_domain
url_host_tld
surt_host_name

301 targets are not in the logfile
'''

'''
domain rollup

unique hosts -- hyperloglog?
tld
url_host_registered_domain
surt_host_name

in federal .gov csv file?

hcrank
'''

if __name__ == '__main__':
    utils.set_memory_limit_gbytes(128)
    main()

