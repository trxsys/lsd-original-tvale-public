# Lazy State Determination (LSD)

The concurrency control algorithms in transactional systems limits concurrency to provide strong semantics, which leads to poor performance under high contention. Consequently, many transactional systems eschew strong semantics to achieve acceptable performance. By leveraging semantic information associated with the transactional programs to increase concurrency, it is possible to significantly improve performance while maintaining linearizability. 

The lazy state determination API allows to easily expose the semantics of application transactions to the database, and propose new optimistic and pessimistic concurrency control algorithms that leverage this information to safely increase concurrency in the presence of contention. 

Our evaluation shows that our approach can achieve up to 5× more throughput with 1.5× less latency than standard techniques in the popular TPC-C benchmark.

## Intalling

1. thrift/compile.sh
2. scripts/compile.sh debug|release #threads

## Acknowledgments

This work was partially funded by the FCT-MEC project HiPSTr (High-performance Software Transactions — PTDC/CCI-COM/32456/2017)&LISBOA-01-0145-FEDER-032456)
