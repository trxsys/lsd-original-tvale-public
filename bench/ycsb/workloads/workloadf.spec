# Yahoo! Cloud System Benchmark
# Workload F: Read-modify-write workload
#   Application example: user database, where user records are read and modified by the user or to record user activity.
#                        
#   Read/read-modify-write ratio: 50/50
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
operationcount=10000
# number of operations per transaction
operationspertransaction=5
# number of fields per value
fieldcount=1
# size of each value's field
fieldlength=32

# workload
readproportion=0.5
updateproportion=0
scanproportion=0
insertproportion=0
readmodifywriteproportion=0.5
requestdistribution=zipfian

