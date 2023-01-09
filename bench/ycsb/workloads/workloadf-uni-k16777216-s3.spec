# Yahoo! Cloud System Benchmark
# Workload F: Read-modify-write workload
#   Application example: user database, where user records are read and modified by the user or to record user activity.
#                        
#   Read/read-modify-write ratio: 50/50
#   Data size: 48 B records (32-byte value, 16-byte key)
#   Request distribution: uniform

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
updateproportion=0
scanproportion=0
insertproportion=0
readmodifywriteproportion=0.5
requestdistribution=uniform

