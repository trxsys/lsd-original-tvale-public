# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Data size: 48 B records (32-byte value, 16-byte key)
#   Request distribution: zipfian
workload=com.yahoo.ycsb.workloads.CoreWorkload
readallfields=true
writeallfields=true
insertorder=ordered
operationcount=0

# datastore size
recordcount=16777216
# number of operations per transaction
operationspertransaction=3
# number of fields per value
fieldcount=1
# size of each value's field
fieldlength=32

# workload
readproportion=0.5
updateproportion=0.5
scanproportion=0
insertproportion=0
readmodifywriteproportion=0
requestdistribution=zipfian

