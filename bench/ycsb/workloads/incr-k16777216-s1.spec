#   RMW ratio: 100
#   Data size: ?? B records (??-byte value, 16-byte key)
#   Request distribution: zipfian
workload=com.yahoo.ycsb.workloads.CoreWorkload
readallfields=true
writeallfields=true
insertorder=ordered
operationcount=0

# datastore size
recordcount=16777216
# number of operations per transaction
operationspertransaction=1
# workload
requestdistribution=zipfian

# number of fields per value (unused)
fieldcount=1
# size of each value's field (unused)
fieldlength=32
# workload (unused)
readproportion=0.0
updateproportion=0.0
scanproportion=0
insertproportion=0
readmodifywriteproportion=0

