## Run

go run main.go --pattern=*.tsv.gz --dryRun

### Flags

--t # run protobuf test <br/>
--log # path to log file <br/>
--dryRun # debug mode <br/>
--pattern # pattern for searching log files <br/>
--idfa # address of idfa memcache server <br/>
--gaid # address of idfa memcache server <br/>
--adid # address of idfa memcache server <br/>
--dvid # address of idfa memcache server <br/>
--workers #number of workers
-- duration # time for retry in second
--timeout  # socket timeout in milliseconds
-- retry # number of retry to set data in memcache
