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

Please see the Makefile for an idea of how to run things.

If you're involved with the EoT 2024 collaboration, you can
ask for [permission to see or download the log dataset on
Hugging Face 🤗](https://huggingface.co/datasets/commoncrawl/eot2024_hostlevel_logs).



