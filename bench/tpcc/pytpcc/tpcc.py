#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http:##www.cs.brown.edu/~pavlo/
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

import sys
sys.path.append('../../../lib/python2.7/site-packages')
sys.path.append('../../../lib/python2.7/site-packages/protobuf-3.1.0-py2.7.egg')
sys.path.append('../../../lib/python2.7/site-packages/six-1.10.0-py2.7.egg')
sys.path.append('../../../thrift/gen-py')
sys.path.append('../../../protobuf')
sys.path.append('../../..')
import os
import string
import datetime
import logging
import re
import argparse
import glob
import time
import multiprocessing
import pickle
from ConfigParser import SafeConfigParser
from pprint import pprint,pformat

from util import *
from runtime import *
import drivers
from hdrh.histogram import HdrHistogram
import constants

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)

## ==============================================
## createDriverClass
## ==============================================
def createDriverClass(name):
    full_name = "%sDriver" % name.title()
    mod = __import__('drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
    klass = getattr(mod, full_name)
    return klass
## DEF

## ==============================================
## getDrivers
## ==============================================
def getDrivers():
    drivers = [ ]
    for f in map(lambda x: os.path.basename(x).replace("driver.py", ""), glob.glob("./drivers/*driver.py")):
        if f != "abstract": drivers.append(f)
    return (drivers)
## DEF

## ==============================================
## startLoading
## ==============================================
def startLoading(driverClass, scaleParameters, args, config):
    logging.debug("Creating client pool with %d processes" % args['clients'])
    pool = multiprocessing.Pool(args['clients'])
    debug = logging.getLogger().isEnabledFor(logging.DEBUG)
    # Split the warehouses into chunks
    w_ids = map(lambda x: [ ], range(args['clients']))
    for w_id in range(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse+1):
        idx = w_id % args['clients']
        w_ids[idx].append(w_id)
    ## FOR
    loader_results = [ ]
    for i in range(args['clients']):
        r = pool.apply_async(loaderFunc, (driverClass, scaleParameters, args, config, w_ids[i], True))
        loader_results.append(r)
    ## FOR
    pool.close()
    logging.debug("Waiting for %d loaders to finish" % args['clients'])
    pool.join()
## DEF

## ==============================================
## loaderFunc
## ==============================================
def loaderFunc(driverClass, scaleParameters, args, config, w_ids, debug):
    driver = driverClass(args['ddl'])
    assert driver != None
    logging.debug("Starting client execution: %s [warehouses=%d]" % (driver, len(w_ids)))
    config['load'] = True
    config['execute'] = False
    config['reset'] = False
    driver.loadConfig(config)
    try:
        loadItems = (1 in w_ids)
        l = loader.Loader(driver, scaleParameters, w_ids, loadItems)
        driver.loadStart()
        l.execute()
        driver.loadFinish()
    except KeyboardInterrupt:
        return -1
    except (Exception, AssertionError), ex:
        logging.warn("Failed to load data: %s" % (ex))
        #if debug:
        traceback.print_exc(file=sys.stdout)
        raise
## DEF

## ==============================================
## startExecution
## ==============================================
def startExecution(driverClass, scaleParameters, args, config):
    logging.debug("Creating client pool with %d processes" % args['clients'])
    pool = multiprocessing.Pool(args['clients'])
    debug = logging.getLogger().isEnabledFor(logging.DEBUG)
    worker_results = [ ]
    for i in range(args['clients']):
        r = pool.apply_async(executorFunc, (driverClass, scaleParameters, args, config, debug,))
        worker_results.append(r)
    ## FOR
    pool.close()
    pool.join()
    total_results = results.Results()
    for asyncr in worker_results:
        asyncr.wait()
        r = asyncr.get()
        assert r != None, "No results object returned!"
        if type(r) == int and r == -1: sys.exit(1)
        total_results.append(r)
    ## FOR
    return (total_results)
## DEF

## ==============================================
## executorFunc
## ==============================================
def executorFunc(driverClass, scaleParameters, args, config, debug):
    driver = driverClass(args['ddl'])
    assert driver != None
    logging.debug("Starting client execution: %s" % driver)
    config['execute'] = True
    config['reset'] = False
    driver.loadConfig(config)
    e = executor.Executor(driver, scaleParameters, stop_on_error=args['stop_on_error'])
    driver.executeStart()
    results = e.execute(args['duration'])
    driver.executeFinish()
    return results
## DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Python implementation of the TPC-C Benchmark')
    aparser.add_argument('system', choices=getDrivers(),
                         help='Target system driver')
    aparser.add_argument('--config', type=file,
                         help='Path to driver configuration file')
    aparser.add_argument('--reset', action='store_true',
                         help='Instruct the driver to reset the contents of the database')
    aparser.add_argument('--scalefactor', default=1, type=float, metavar='SF',
                         help='Benchmark scale factor')
    aparser.add_argument('--warehouses', default=4, type=int, metavar='W',
                         help='Number of Warehouses')
    aparser.add_argument('--duration', default=60, type=int, metavar='D',
                         help='How long to run the benchmark in seconds')
    aparser.add_argument('--warmup', default=10, type=int,
                         help='How long to warmup/cooldown the benchmark in seconds')
    aparser.add_argument('--ddl', default=os.path.realpath(os.path.join(os.path.dirname(__file__), "tpcc.sql")),
                         help='Path to the TPC-C DDL SQL file')
    aparser.add_argument('--clients', default=1, type=int, metavar='N',
                         help='The number of blocking clients to fork')
    aparser.add_argument('--stop-on-error', action='store_true',
                         help='Stop the transaction execution when the driver throws an exception.')
    aparser.add_argument('--no-load', action='store_true',
                         help='Disable loading the data')
    aparser.add_argument('--no-execute', action='store_true',
                         help='Disable executing the workload')
    aparser.add_argument('--print-config', action='store_true',
                         help='Print out the default configuration file for the system and exit')
    aparser.add_argument('--debug', action='store_true',
                         help='Enable debug log messages')
    aparser.add_argument('--nurand', default=None,
                         help='NURand seeds used for loading database')
    args = vars(aparser.parse_args())
    if args['debug']: logging.getLogger().setLevel(logging.DEBUG)
    ## Create a handle to the target client driver
    driverClass = createDriverClass(args['system'])
    assert driverClass != None, "Failed to find '%s' class" % args['system']
    driver = driverClass(args['ddl'])
    assert driver != None, "Failed to create '%s' driver" % args['system']
    if args['print_config']:
        config = driver.makeDefaultConfig()
        print driver.formatConfig(config)
        print
        sys.exit(0)
    ## Load Configuration file
    if args['config']:
        logging.debug("Loading configuration file '%s'" % args['config'])
        cparser = SafeConfigParser()
        cparser.read(os.path.realpath(args['config'].name))
        config = dict(cparser.items('tpcc'))
    else:
        logging.debug("Using default configuration for %s" % args['system'])
        defaultConfig = driver.makeDefaultConfig()
        config = dict(map(lambda x: (x, defaultConfig[x][1]), defaultConfig.keys()))
    config['reset'] = args['reset']
    config['load'] = False
    config['execute'] = False
    if config['reset']: logging.info("Reseting database")
    driver.loadConfig(config)
    logging.info("Initializing TPC-C benchmark using %s" % driver)
    ## Create ScaleParameters
    scaleParameters = scaleparameters.makeWithScaleFactor(args['warehouses'], args['scalefactor'])
    if args['nurand'] is None:
      nurand = nurand.makeForLoad()
    else:
      f = open(args['nurand'], 'rb')
      nurand = pickle.load(f)
      f.close()
    rand.setNURand(nurand)
    if args['debug']: logging.debug("Scale Parameters:\n%s" % scaleParameters)
    ## DATA LOADER!!!
    load_time = None
    if not args['no_load']:
        logging.info("Loading TPC-C benchmark data using %s" % (driver))
        load_start = time.time()
        if args['clients'] == 1:
            l = loader.Loader(driver, scaleParameters, range(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse+1), True)
            driver.loadStart()
            l.execute()
            driver.loadFinish()
        else:
            startLoading(driverClass, scaleParameters, args, config)
        load_time = time.time() - load_start
    ## IF
    ## WORKLOAD DRIVER!!!
    if not args['no_execute']:
        if args['clients'] == 1:
            e = executor.Executor(driver, scaleParameters, stop_on_error=args['stop_on_error'])
            driver.executeStart()
            results = e.execute(args['duration'], args['warmup'])
            driver.executeFinish()
        else:
            results = startExecution(driverClass, scaleParameters, args, config)
        assert results
        # print results.show(load_time)
        hdr = HdrHistogram(1, 1000000, 3)
        d = constants.TransactionTypes.DELIVERY
        no = constants.TransactionTypes.NEW_ORDER
        os = constants.TransactionTypes.ORDER_STATUS
        p = constants.TransactionTypes.PAYMENT
        sl = constants.TransactionTypes.STOCK_LEVEL
        sum_commits = 0
        sum_aborts = 0
        for t in [d, no, os, p, sl]:
            sum_commits += results.txn_counters[t]
            sum_aborts  += results.txn_aborts[t]
            hdr.add(results.txn_times[t])
        throughput = sum_commits / args['duration']
        total = (sum_commits + sum_aborts) / args['duration']
        print '[raw]'
        print 'commited = {}'.format(sum_commits)
        print 'aborted = {}'.format(sum_aborts)
        print 'total = {}'.format(sum_commits + sum_aborts)
        print '[throughput]'
        print 'commits/s = {}'.format(throughput)
        print 'txn/s = {}'.format(total)
        print '[latency]'
        print 'lat-min = {}'.format(hdr.get_min_value())
        print 'lat-mean = {}'.format(hdr.get_mean_value())
        print 'lat-median = {}'.format(hdr.get_value_at_percentile(50))
        print 'lat-99 = {}'.format(hdr.get_value_at_percentile(99))
        print 'lat-99.9 = {}'.format(hdr.get_value_at_percentile(99.9))
        print 'lat-99.99 = {}'.format(hdr.get_value_at_percentile(99.99))
        print 'lat-99.999 = {}'.format(hdr.get_value_at_percentile(99.999))
        print 'lat-max = {}'.format(hdr.get_max_value())
        print 'lat-raw =',
        it = hdr.get_recorded_iterator()
        for item in it:
            print ' {}:{}'.format(item.value_iterated_to,
                                  item.total_count_to_this_value),
        print ''
    ## IF
## MAIN
