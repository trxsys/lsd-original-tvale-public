#   RMW ratio: 100
#   Data size: ?? B records (??-byte value, 16-byte key)
#   Request distribution: hotkey
workload=com.yahoo.ycsb.workloads.CoreWorkload
readallfields=true
writeallfields=true
insertorder=ordered
operationcount=0

# datastore size
recordcount=16777216
# percentage of transactions that access hot key
hotkeypercentage=80

# number of operations per transaction (unused)
operationspertransaction=1
# workload (unused)
requestdistribution=uniform
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

