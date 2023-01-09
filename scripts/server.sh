#!/bin/bash

if [[ "$#" -ne 2 ]]; then
    echo "usage: $0 (occ|2pl)[-batch] (id)"
    exit 1
fi

rm -r rocks_db_$2

LD_LIBRARY_PATH=lib/thrift-0.9.3/lib:lib/HyperLevelDB-releases-1.2.2/.libs \
    ./dist/server-$1 $2 >server-$1.$2.out.txt 2>server-$1.$2.err.txt
