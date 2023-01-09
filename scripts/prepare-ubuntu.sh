#!/bin/sh

# thrift dependencies
sudo apt-get install libboost-dev libboost-test-dev \
    libboost-program-options-dev libboost-system-dev libboost-filesystem-dev \
    libevent-dev automake libtool flex bison pkg-config g++ libssl-dev \
    libboost-thread-dev make
sudo apt-get install python-all python-all-dev python-all-dbg
# rocksdb dependencies
sudo apt-get install zlib1g-dev libsnappy-dev libbz2-dev
# protobuf
sudo apt-get install protobuf-compiler python-protobuf

# runtime dependencies
sudo apt-get install python-pip dtach tmux
sudo pip install fabric execnet
