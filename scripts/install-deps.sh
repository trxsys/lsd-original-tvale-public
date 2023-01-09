#!/bin/bash

mkdir -p dist

LSD_HOME=`pwd`
cd lib

if [ ! -d "thrift-0.9.3" ]; then
  wget http://www-eu.apache.org/dist/thrift/0.9.3/thrift-0.9.3.tar.gz
  tar -axf thrift-0.9.3.tar.gz
  rm thrift-0.9.3.tar.gz
  LIB_DIR=`pwd`
  patch -p0 <../thrift/THttpClient.py.patch
  cd thrift-0.9.3
  PY_PREFIX=$LSD_HOME ./configure --prefix=$LIB_DIR/thrift --with-python --without-java --without-haskell
  make -j4
  make install
  cd ..
  rm -rf thrift-0.9.3
  mv thrift thrift-0.9.3
fi

if [ ! -d "rocksdb-4.11.2" ]; then
  wget https://github.com/facebook/rocksdb/archive/v4.11.2.tar.gz
  tar -axf v4.11.2.tar.gz
  rm v4.11.2.tar.gz
  cd rocksdb-4.11.2
  make -j4 static_lib
  cd ..
fi

if [ ! -d "protobuf-3.1.0" ]; then
  wget https://github.com/google/protobuf/releases/download/v3.1.0/protobuf-cpp-3.1.0.tar.gz
  tar -axf protobuf-cpp-3.1.0.tar.gz
  rm protobuf-cpp-3.1.0.tar.gz
  wget https://github.com/google/protobuf/releases/download/v3.1.0/protobuf-python-3.1.0.tar.gz
  tar -axf protobuf-python-3.1.0.tar.gz
  rm protobuf-python-3.1.0.tar.gz
  cd protobuf-3.1.0
  ./configure --disable-shared
  make -j4
  cd python
  PYTHONPATH=$LSD_HOME/lib/python2.7/site-packages python setup.py install --prefix $LSD_HOME
  cd ..
  cd ..
fi

if [ ! -d "HyperLevelDB-releases-1.2.2" ]; then
  wget https://github.com/rescrv/HyperLevelDB/archive/releases/1.2.2.tar.gz
  tar -axf 1.2.2.tar.gz
  rm 1.2.2.tar.gz
  cd HyperLevelDB-releases-1.2.2
  autoreconf -i
  ./configure
  make -j4 libhyperleveldb.la
  cd ..
fi

pip install -t python2.7/site-packages hdrhistogram
