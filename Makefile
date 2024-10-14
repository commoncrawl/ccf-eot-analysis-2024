get-fed-csvs:
	wget https://raw.githubusercontent.com/cisagov/dotgov-data/main/current-federal.csv
	wget https://raw.githubusercontent.com/cisagov/dotgov-data/main/current-full.csv

accumulate-hosts-test:
	# 86.90user 0.45system 1:23.99elapsed 104%CPU (0avgtext+0avgdata 240428maxresident)k
	# in.gz 338MB, out.parquet 2MB
	/bin/time python accumulate-hosts.py crawl803-crawl.log.gz

accumulate-hosts-all:
	# 2143.37user 8.31system 35:48.70elapsed 100%CPU (0avgtext+0avgdata 922692maxresident)k
	# in.gz 8.2GB, out.parquet 12MB
	/bin/time python accumulate-hosts.py crawl*-crawl.log.gz

accumulate-hosts-join:
	# 96.36user 3.49system 0:04.25elapsed 2348%CPU (0avgtext+0avgdata 1010000maxresident)k
	# 12MB in, hostlevel is 6GB, 14 MB out
	/bin/time python ./duck-left-join.py first_log_drop_00000.parquet /home/cc-pds/commoncrawl/cc-index/table/cc-main/hostlevel/crawl\=CC-MAIN-2024-33/CC-MAIN-2024-33-hostlevel.parquet eot2024_hostlevel_logs.parquet
	parquet-tools csv eot2024_hostlevel_logs.parquet > eot2024_hostlevel_logs.csv

accumulate-domains:
	/bin/time python accumulate-domains.py eot2024_hostlevel_logs.parquet
	@echo no domains database to get ranks from
	parquet-tools csv eot2024_domainlevel_logs.parquet > eot2024_domainlevel_logs.csv


count:
	python duckit.py 'select count (*) from eot2024'

captures:
	python duckit.py 'select sum(captures) from eot2024'

tlds:
	python duckit.py 'select url_host_tld, sum(captures) from eot2024 group by url_host_tld order by sum(captures) DESC' > tlds

tlds-captures:
	python duckit.py 'select url_host_tld, sum(captures), sum(ccf_captures) from eot2024 group by url_host_tld order by sum(captures) DESC' > tlds-captures

io:
	python duckit.py "select * from eot2024 where url_host_tld = 'io' limit 10" > io

top-io:
	python duckit.py "select * from eot2024 where url_host_tld = 'io' order by captures desc limit 20" > $@

top-gov-federal:
	python duckit.py "select url_host_name, captures, fed_csv_registered_domain from eot2024 where url_host_tld = 'gov' and fed_csv_registered_domain = 1 order by captures desc limit 20" > $@

top-gov-federal-captures:
	python duckit.py "select sum(captures) from eot2024 where url_host_tld = 'gov' and fed_csv_registered_domain = 1" > $@

top-gov-nonfederal:
	python duckit.py "select url_host_name, captures, fed_csv_registered_domain from eot2024 where url_host_tld = 'gov' and fed_csv_registered_domain = 0 order by captures desc limit 20" > $@

top-gov-nonfederal-captures:
	python duckit.py "select sum(captures) from eot2024 where url_host_tld = 'gov' and fed_csv_registered_domain = 0" > $@

top-gov-robots-fetch-failed:
	# empty? seems to never increment
	python duckit.py "select * from eot2024 where url_host_tld = 'gov' and codes_robots_fetch_failed > 0 order by hcrank desc" > $@

top-gov-robots-disallowed:
	# empty? seems to never increment
	python duckit.py "select * from eot2024 where url_host_tld = 'gov' and codes_robots_disallowed > 0 order by hcrank desc" > $@

debug:
	python duckit.py "select url_host_name, captures, ccf_captures, robots_code, hcrank from eot2024 where url_host_tld = 'gov' and robots_code != '200' and robots_code != '404' order by hcrank desc"

debug2:
	python duckit.py "select url_host_name, captures, ccf_captures, codes_network_failed, hcrank from eot2024 where url_host_tld = 'gov' and codes_network_failed > 0 order by hcrank desc"

robots_codes:
	python duckit.py "select robots_code, count(*) from eot2024 group by robots_code order by count(*) desc" > $@

robots_codes_gov:
	python duckit.py "select robots_code, count(*) from eot2024 where url_host_tld = 'gov' group by robots_code order by count(*) desc" > $@

robots_codes_gov_null:
	python duckit.py "select robots_code, url_host_name from eot2024 where url_host_tld = 'gov' and robots_code is NULL order by hcrank desc" > $@

robots_codes_gov_fishy:
	python duckit.py "select robots_code, url_host_name from eot2024 where url_host_tld = 'gov' and contains(ARRAY ['403', '503', '429'], robots_code) order by hcrank desc" > $@
