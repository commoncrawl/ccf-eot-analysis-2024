# CCF EoT Analysis 2024

The [End of Term Web Archive 2024 crawl](https://github.com/end-of-term/eot2024/)
is being mostly conducted by the Internet Archive using the Heritrix crawler.

This repo contains tools that ingest Heritrix logs, aggregates them at
a web host level, and joins them with Common Crawl's host-level
database ranking information and crawl counts.

The resulting dataset can be used to answer questions like:

- Is the crawl mostly focused on the intended US federal .gov and .mil domains?
- Are there important .gov and .mil websites that appear under-crawled?
- Are there important .gov and .mil websites that are blocking the crawler?

## Makefile

Please see the Makefile for an idea of how to run things. It both builds
the dataset and shows example SQL queries against it.

## Dataset

If you're involved with the EoT 2024 collaboration, you can
ask for [permission to see or download the log dataset on
Hugging Face 🤗](https://huggingface.co/datasets/commoncrawl/eot2024_hostlevel_logs).
Please contact Greg directly on Slack or email, because we're
getting a lot of random requests.

You can also Greg to have the dataset emailed, it's just 12 megabytes

The dataset is also available as a csv imported into a Google Sheet --
a link for this is posted on Slack.

## Schema -- host-level db

- surt_host_name, e.g. 'com.commoncrawl'
- url_host_name, e.g. 'commoncrawl.org'
- url_host_tld, e.g. 'org'
- url_host_registered_domain, e.g. 'commoncrawl.org' for host name index.commoncrawl.org
- fed_csv_registered_domain, 0, or 1 for gov websites if in the federal .gov csv list

- hcrank -- harmonic centrality rank from the web graph
- prank -- page rank from the web graph
- ccf_captures -- the count of webpage captures for this host in CC-MAIN-2024-33 (just 200s)
- captures -- the count of webpage captures for this host in EoT 2024 (all codes, not just 200)

- robots_duration -- milliseconds, for the most recent robots fetch (string)
- robots_code -- http code for the most recent robots fetch (string). -404 was a 200 but empty file.

### HTTP response code counts (integers)

- codes_2xx  # success
- codes_3xx  # redir
- codes_4xx  # temporary error
- codes_5xx  # permanent error
- codes_0  # did not fetch for some Nutch precursor reason (dns, robots)
- codes_999  # unusual we-hate-crawler code
- codes_666  # ditto
- codes_xxx  # catchall

### Failed column ideas

The following column ideas came from the Nutch documentation, but turn
out to never happen. We will remove them in the next iteration.

- codes_minus_4xx
- codes_other_minus
- codes_robots_disallowed
- codes_robots_fetch_failed
- codes_network_failed

## TODOs

- remove zero columns
- csv equivalent to the parquet
- percentage columns, to avoid SQL arithmetic?
- mimetypes counts (grouped by text/ video/ audio/)

- domain-level rollups

