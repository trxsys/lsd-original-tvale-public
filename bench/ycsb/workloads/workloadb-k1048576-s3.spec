# Yahoo! Cloud System Benchmark
# Workload B: Read mostly workload
#   Application example: photo tagging; add a tag is an update, but most operations are to read tags
#                        
#   Read/update ratio: 95/5
#   Default data size: 48 B records (32-byte value, 16-byte key)
#   Request distribution: zipfian

workload=com.yahoo.ycsb.workloads.CoreWorkload
readallfields=true
writeallfields=true
insertorder=ordered
operationcount=0

# datastore size
recordcount=1048576
# number of operations per transaction
operationspertransaction=3
# number of fields per value
fieldcount=1
# size of each value's field
fieldlength=32

# workload
readproportion=0.95
updateproportion=0.05
scanproportion=0
insertproportion=0
readmodifywriteproportion=0
requestdistribution=zipfian

