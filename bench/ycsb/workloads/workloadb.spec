# Yahoo! Cloud System Benchmark
# Workload B: Read mostly workload
#   Application example: photo tagging; add a tag is an update, but most operations are to read tags
#                        
#   Read/update ratio: 95/5
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

workload=com.yahoo.ycsb.workloads.CoreWorkload
readallfields=true
writeallfields=true
insertorder=ordered

# number of records to insert...
recordcount=10000
# ...starting at 
insertstart=0
# number of operations to execute
operationcount=1000
# number of operations per transaction
operationspertransaction=5
# number of fields per value
fieldcount=1
# size of each value's field
fieldlength=32

# workload
readproportion=0.95
updateproportion=0.05
scanproportion=0
insertproportion=0
requestdistribution=zipfian

