//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "../../barrier/client.hh"
#include "../../common/config.hh"
#include "core/client.h"
#include "core/core_workload.h"
#include "core/db.h"
#include "core/timer.h"
#include "core/utils.h"
#include <atomic>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <future>
#include <hdr/hdr_histogram.h>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace std;

namespace lsd = novalincs::cs::lsd;

int constexpr WARMUP = 0;
int constexpr MEASURE = 1;
int constexpr COOLDOWN = 2;

void UsageMessage(const char* command);
bool StrStartWith(const char* str, const char* pre);
string ParseCommandLine(int argc, const char* argv[], utils::Properties& props);

std::pair<int, int>
AssertClient(ycsbc::DB* db, ycsbc::CoreWorkload* wl, int const num_ops,
             bool const is_loading, struct hdr_histogram* hist,
             utils::Properties const& p, double& duration,
             std::atomic_bool& stop, std::atomic_int& state_)
{
  using novalincs::cs::lsd::lsd_client;
  using novalincs::cs::lsd::txn_exception;
  using novalincs::cs::lsd::klist_t;
  using novalincs::cs::lsd::ftype_t;
  using novalincs::cs::lsd::future_t;
  using novalincs::cs::lsd::predicate_t;
  using novalincs::cs::lsd::function_t;
  using novalincs::cs::lsd::fbinaryop_t;
  using novalincs::cs::lsd::fptr_t;
  using novalincs::cs::lsd::fptr_map_t;
  using novalincs::cs::lsd::result_t;
  using novalincs::cs::lsd::result_map_t;
  using novalincs::cs::lsd::assumption;

  auto const uid = std::stoi(p.GetProperty("globalid"));
  auto const hot_key_perc = wl->GetHotKeyPercentage();
  auto const default_value = wl->GetCounterInitialValue();
  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  lsd_client txn(boost::uuids::to_string(uuid));
  int oks = 0;
  int total = 0;
  if (is_loading) {
    txn.begin();
    for (int i = 0; i < num_ops; ++i) {
      ++total;
      std::string const key = wl->NextSequenceKey();
      txn.put(key, std::to_string(default_value));
      ++oks;
    }
    txn.commit();
  } else {
    bool commit;
    utils::Timer<double> timer;
    std::mt19937 gen(uid);
    std::uniform_int_distribution<> dist(1, 100);
    timer.Start();
    while (!stop.load(std::memory_order_relaxed)) {
      std::string key;
      auto const p = dist(gen);
      if (p <= hot_key_perc) {
        key = wl->BuildKeyName(0);
      } else {
        key = wl->BuildKeyName(uid + 1);
      }
      commit = false;
      auto t1 = std::chrono::high_resolution_clock::now();
      while (!commit && !stop.load(std::memory_order_relaxed)) {
        try {
          txn.begin();
#ifdef YCSB_LSD
          auto f = txn.get_lsd(key);
          // predicate
          future_t gte_op;
          gte_op.type = ftype_t::GTEQ_FI;
          fbinaryop_t& gte_bin_op = gte_op.u.binaryop;
          gte_bin_op.resolved = false;
          gte_bin_op.left.fi = f.u.fi;
          gte_bin_op.right.value = "0";
          auto gte_op_fp = txn.make_future(gte_op);
          predicate_t gte_pred;
          function_t& gte_func = gte_pred.func;
          gte_func.keys = { key };
          gte_func.future_fi = gte_op_fp.u.fi;
#ifdef YCSB_ASSERT_BENCH_ASSUME
          bool pred_is_true = txn.is_true(gte_pred, assumption::TRUE);
#else
          bool pred_is_true = txn.is_true(gte_pred);
#endif
          if (pred_is_true) {
            future_t sub_op;
            sub_op.type = ftype_t::SUB_FI;
            fbinaryop_t& sub_bin_op = sub_op.u.binaryop;
            sub_bin_op.resolved = false;
            sub_bin_op.left.fi = f.u.fi;
            sub_bin_op.right.value = "1";
            auto sub_op_fp = txn.make_future(sub_op);
            txn.put_lsd(key, sub_op_fp);
          } else {
            txn.put(key, std::to_string(default_value - 1));
          }
#else
          auto result = txn.get(key, true);
          assert(result.exists);
          auto value = std::stoi(result.value);
          if (value > 0) {
            auto new_value = std::to_string(value - 1);
            txn.put(key, new_value);
          } else {
            txn.put(key, std::to_string(default_value - 1));
          }
#endif
          txn.commit();
          commit = true;
        } catch (txn_exception const& e) {
          if (state_.load(std::memory_order_relaxed) == MEASURE) {
            ++total;
          }
        }
      }
      if (state_.load(std::memory_order_relaxed) == MEASURE) {
        if (commit) {
          ++oks;
          auto t2 = std::chrono::high_resolution_clock::now();
          auto us =
            std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
          hdr_record_value(hist, us.count());
        }
        ++total;
      }
    }
    duration = timer.End();
  }
  return std::make_pair(oks, total);
}

std::pair<int, int>
HotKeyClient(ycsbc::DB* db, ycsbc::CoreWorkload* wl, int const num_ops,
             bool const is_loading, struct hdr_histogram* hist,
             utils::Properties const& p, double& duration,
             std::atomic_bool& stop, std::atomic_int& state_)
{
  using novalincs::cs::lsd::lsd_client;
  using novalincs::cs::lsd::txn_exception;
  using novalincs::cs::lsd::klist_t;
  using novalincs::cs::lsd::ftype_t;
  using novalincs::cs::lsd::future_t;
  using novalincs::cs::lsd::fbinaryop_t;
  using novalincs::cs::lsd::fptr_t;
  using novalincs::cs::lsd::fptr_map_t;
  using novalincs::cs::lsd::result_t;
  using novalincs::cs::lsd::result_map_t;

  auto const uid = std::stoi(p.GetProperty("globalid"));
  auto const hot_key_perc = wl->GetHotKeyPercentage();
  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  lsd_client txn(boost::uuids::to_string(uuid));
  int oks = 0;
  int total = 0;
  if (is_loading) {
    txn.begin();
    for (int i = 0; i < num_ops; ++i) {
      ++total;
      std::string const key = wl->NextSequenceKey();
      txn.put(key, "0");
      ++oks;
    }
    txn.commit();
  } else {
    bool commit;
    utils::Timer<double> timer;
    std::mt19937 gen(uid);
    std::uniform_int_distribution<> dist(1, 100);
    timer.Start();
    while (!stop.load(std::memory_order_relaxed)) {
      std::string key;
      auto const p = dist(gen);
      if (p <= hot_key_perc) {
        key = wl->BuildKeyName(0);
      } else {
        key = wl->BuildKeyName(uid + 1);
      }
      commit = true;
      auto t1 = std::chrono::high_resolution_clock::now();
      try {
        txn.begin();
#ifdef YCSB_LSD
        auto f = txn.get_lsd(key);
        future_t add_op;
        add_op.type = ftype_t::ADD_FI;
        fbinaryop_t& add_bin_op = add_op.u.binaryop;
        add_bin_op.resolved = false;
        add_bin_op.left.fi = f.u.fi;
        add_bin_op.right.value = "1";
        auto add_op_fp = txn.make_future(add_op);
        txn.put_lsd(key, add_op_fp);
#else
        auto result = txn.get(key, true);
        assert(result.exists);
        auto value = result.value;
        auto new_value = std::to_string(std::stoi(value) + 1);
        txn.put(key, new_value);
#endif
        txn.commit();
      } catch (txn_exception const& e) {
        commit = false;
      }
      if (state_.load(std::memory_order_relaxed) == MEASURE) {
        if (commit) {
          ++oks;
          auto t2 = std::chrono::high_resolution_clock::now();
          auto us =
            std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
          hdr_record_value(hist, us.count());
        }
        ++total;
      }
    }
    duration = timer.End();
  }
  return std::make_pair(oks, total);
}

std::pair<int, int>
IncrementClient(ycsbc::DB* db, ycsbc::CoreWorkload* wl, int const num_ops,
                bool const is_loading, struct hdr_histogram* hist,
                utils::Properties const& p, double& duration,
                std::atomic_bool& stop, std::atomic_int& state_)
{
  using novalincs::cs::lsd::lsd_client;
  using novalincs::cs::lsd::txn_exception;
  using novalincs::cs::lsd::klist_t;
  using novalincs::cs::lsd::ftype_t;
  using novalincs::cs::lsd::future_t;
  using novalincs::cs::lsd::fbinaryop_t;
  using novalincs::cs::lsd::fptr_t;
  using novalincs::cs::lsd::fptr_map_t;
  using novalincs::cs::lsd::result_t;
  using novalincs::cs::lsd::result_map_t;

  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  lsd_client txn(boost::uuids::to_string(uuid));
  int oks = 0;
  int total = 0;
  if (is_loading) {
    txn.begin();
    for (int i = 0; i < num_ops; ++i) {
      ++total;
      std::string const key = wl->NextSequenceKey();
      txn.put(key, "0");
      ++oks;
    }
    txn.commit();
  } else {
    bool commit;
    utils::Timer<double> timer;
    timer.Start();
    while (!stop.load(std::memory_order_relaxed)) {
      std::unordered_set<std::string> keys_;
      do {
        keys_.insert(wl->NextTransactionKey());
      } while (keys_.size() < wl->ops_per_txn());
      commit = true;
      auto t1 = std::chrono::high_resolution_clock::now();
      try {
        txn.begin();
        for (auto const& key : keys_) {
#ifdef YCSB_LSD
          auto f = txn.get_lsd(key);
          future_t add_op;
          add_op.type = ftype_t::ADD_FI;
          fbinaryop_t& add_bin_op = add_op.u.binaryop;
          add_bin_op.resolved = false;
          add_bin_op.left.fi = f.u.fi;
          add_bin_op.right.value = "1";
          auto add_op_fp = txn.make_future(add_op);
          txn.put_lsd(key, add_op_fp);
#else
          auto result = txn.get(key, true);
          assert(result.exists);
          auto value = result.value;
          auto new_value = std::to_string(std::stoi(value) + 1);
          txn.put(key, new_value);
#endif
        }
        txn.commit();
      } catch (txn_exception const& e) {
        commit = false;
      }
      if (state_.load(std::memory_order_relaxed) == MEASURE) {
        if (commit) {
          ++oks;
          auto t2 = std::chrono::high_resolution_clock::now();
          auto us =
            std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
          hdr_record_value(hist, us.count());
        }
        ++total;
      }
    }
    duration = timer.End();
  }
  return std::make_pair(oks, total);
}

std::pair<int, int>
YCSBClient(ycsbc::DB* db, ycsbc::CoreWorkload* wl, int const num_ops,
           bool const is_loading, struct hdr_histogram* hist,
           utils::Properties const& p, double& duration, std::atomic_bool& stop,
           std::atomic_int& state_)
{
  db->Init(p);
  ycsbc::Client client(*db, *wl);
  int oks = 0;
  int total = 0;
  if (is_loading) {
    db->Begin();
    for (int i = 0; i < num_ops; ++i) {
      ++total;
      oks += client.DoInsert();
    }
    db->Commit();
  } else {
    bool commit;
    utils::Timer<double> timer;
    timer.Start();
    while (!stop.load(std::memory_order_relaxed)) {
      auto t1 = std::chrono::high_resolution_clock::now();
      commit = client.DoTransaction();
      if (state_.load(std::memory_order_relaxed) == MEASURE) {
        if (commit) {
          ++oks;
          auto t2 = std::chrono::high_resolution_clock::now();
          auto us =
            std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
          hdr_record_value(hist, us.count());
        }
        ++total;
      }
    }
    duration = timer.End();
  }
  db->Close();
  return std::make_pair(oks, total);
}

int
main(const int argc, const char* argv[])
{
  utils::Properties props;
  string file_name = ParseCommandLine(argc, argv, props);
  lsd::config cfg;

  int const nrecords = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
  int const client_id = stoi(props["clientid"]);
  auto const nclients = cfg.clients().size();
  auto const total_threads = cfg.total_threads();
  auto const threads = cfg.threads(client_id);
  auto const nservers = cfg.servers().size();
  cerr << "client id: " << client_id << endl;
  cerr << "client threads: " << threads << endl;
  cerr << "total threads: " << total_threads << endl;
  assert(nrecords % 2 == 0);
  assert(total_threads % 2 == 0 || total_threads == 1);
  assert(threads % 2 == 0 || threads == 1);

  auto* tprops = new utils::Properties[threads];
  auto* db = new ycsbc::DB[threads];
  auto* wl = new ycsbc::CoreWorkload[threads];
  auto** hist = new struct hdr_histogram*[threads];
  auto* durations = new double[threads];
  auto* commits = new int[threads];
  auto* issued = new int[threads];
  auto* flags = new std::atomic_bool[threads];
  std::atomic_int state_;
  state_.store(WARMUP);
  for (auto i = 0; i < threads; i++) {
    auto const id = cfg.global_id(client_id, i);
    int const insertstart = (nrecords / total_threads) * id;
    tprops[i] = props;
    tprops[i].SetProperty("insertstart", to_string(insertstart));
    tprops[i].SetProperty("globalid", to_string(id));
    wl[i].Init(tprops[i]);
    flags[i].store(false);
    auto const err = hdr_init(1, 1000000,
                              3, // 3 significant figures, e.g. 99.999% ?
                              &hist[i]);
    if (err != 0) {
      cerr << "hdr_init returned non-zero" << endl;
      exit(1);
    }
  }

  int sum_commits = 0;
  int sum_issued = 0;
  vector<future<std::pair<int, int>>> actual_ops;
  auto const no_load = stoi(props.GetProperty("no_load", "0"));
  auto const no_exec = stoi(props.GetProperty("no_exec", "0"));

  lsd::barrier_client barrier(cfg.barrier_host(), cfg.barrier_port());
  auto const barrier_init =
    "server-init-" + std::to_string(nservers + nclients);
  barrier.create(barrier_init, nservers + nclients);
  barrier.wait(barrier_init);

  if (!no_load) {
    // Loads data
    int total_ops = nrecords / total_threads;
    int insertstart = total_ops * cfg.global_id(client_id, 0);
    int insertstop = total_ops * cfg.global_id(client_id, threads);

    cerr << "inserting records ";
    cerr << "[" << insertstart << "," << insertstop << "]";
    cerr << "/" << nrecords << "... ";

    for (int i = 0; i < threads; ++i) {
#if defined YCSB_INCR_BENCH
      actual_ops.emplace_back(
        async(launch::async, IncrementClient, &db[i], &wl[i],
#elif defined YCSB_HOTKEY_BENCH
      actual_ops.emplace_back(
        async(launch::async, HotKeyClient, &db[i], &wl[i],
#elif defined YCSB_ASSERT_BENCH
      actual_ops.emplace_back(
        async(launch::async, AssertClient, &db[i], &wl[i],
#else
      actual_ops.emplace_back(
        async(launch::async, YCSBClient, &db[i], &wl[i],
#endif
              total_ops, true, nullptr, std::ref(tprops[i]),
              std::ref(durations[i]), std::ref(flags[i]), std::ref(state_)));
    }
    assert((int)actual_ops.size() == threads);

    for (auto& n : actual_ops) {
      assert(n.valid());
      auto const& result = n.get();
      sum_commits += result.first;
      sum_issued += result.second;
    }
    assert(sum_commits == insertstop - insertstart);

    cerr << "done" << endl;
  }

  actual_ops.clear();

  if (!no_exec) {
    auto const barrier_init = "ycsb-init-" + to_string(nclients);
    auto const barrier_stop = "ycsb-stop-" + to_string(nclients);
    barrier.create(barrier_init, nclients);
    barrier.create(barrier_stop, nclients);

    cerr << "waiting for other clients... ";
    barrier.wait(barrier_init);
    cerr << "done" << endl;

    // Peforms transactions
    for (int i = 0; i < threads; ++i) {
#if defined YCSB_INCR_BENCH
      actual_ops.emplace_back(
        async(launch::async, IncrementClient, &db[i], &wl[i],
#elif defined YCSB_HOTKEY_BENCH
      actual_ops.emplace_back(
        async(launch::async, HotKeyClient, &db[i], &wl[i],
#elif defined YCSB_ASSERT_BENCH
      actual_ops.emplace_back(
        async(launch::async, AssertClient, &db[i], &wl[i],
#else
      actual_ops.emplace_back(
        async(launch::async, YCSBClient, &db[i], &wl[i],
#endif
              0, false, hist[i], std::ref(tprops[i]), std::ref(durations[i]),
              std::ref(flags[i]), std::ref(state_)));
    }
    assert((int)actual_ops.size() == threads);

    auto const wc_duration = stoi(props.GetProperty("warmup"));
    auto const exp_duration = stoi(props.GetProperty("duration"));

    cerr << "warmup for " << wc_duration << " seconds... ";
    std::this_thread::sleep_for(std::chrono::seconds(wc_duration));
    cerr << "done" << endl;

    state_.store(MEASURE, std::memory_order_relaxed);
    cerr << "measure for " << exp_duration << " seconds... ";
    std::this_thread::sleep_for(std::chrono::seconds(exp_duration));
    cerr << "done" << endl;

    state_.store(COOLDOWN, std::memory_order_relaxed);
    cerr << "cooldown for " << wc_duration << " seconds... ";
    std::this_thread::sleep_for(std::chrono::seconds(wc_duration));
    cerr << "done" << endl;

    for (int i = 0; i < threads; ++i) {
      flags[i].store(true, std::memory_order_relaxed);
    }

    sum_commits = 0;
    sum_issued = 0;
    for (int i = 0; i < threads; ++i) {
      auto& n = actual_ops[i];
      assert(n.valid());
      auto const& result = n.get();
      commits[i] = result.first;
      sum_commits += commits[i];
      issued[i] = result.second;
      sum_issued += issued[i];
    }

    auto throughput = sum_commits / exp_duration;
    auto total = sum_issued / exp_duration;
    for (int i = 1; i < threads; ++i) {
      hdr_add(hist[0], hist[i]);
    }

    cout << "[raw]" << endl;
    cout << "commited = " << sum_commits << endl;
    cout << "aborted = " << sum_issued - sum_commits << endl;
    cout << "total = " << sum_issued << endl;
    cout << "[throughput]" << endl;
    cout << "commits/s = " << throughput << endl;
    cout << "txn/s = " << total << endl;
    cout << "[latency]" << endl;
    cout << "lat-min = " << hdr_min(hist[0]) << endl;
    cout << "lat-mean = " << hdr_mean(hist[0]) << endl;
    cout << "lat-median = " << hdr_value_at_percentile(hist[0], 50) << endl;
    cout << "lat-99 = " << hdr_value_at_percentile(hist[0], 99) << endl;
    cout << "lat-99.9 = " << hdr_value_at_percentile(hist[0], 99.9) << endl;
    cout << "lat-99.99 = " << hdr_value_at_percentile(hist[0], 99.99) << endl;
    cout << "lat-99.999 = " << hdr_value_at_percentile(hist[0], 99.999) << endl;
    cout << "lat-max = " << hdr_max(hist[0]) << endl;
    cout << "lat-raw =";
    struct hdr_iter it;
    hdr_iter_recorded_init(&it, hist[0]);
    while (hdr_iter_next(&it)) {
      cout << " " << it.value << ":" << it.cumulative_count;
    }
    cout << endl;
    cout.flush();

    cerr << "waiting for other clients... ";
    barrier.wait(barrier_stop);
    cerr << "done" << endl;

    barrier.destroy(barrier_init);
    barrier.destroy(barrier_stop);
  }

  barrier.destroy(barrier_init);

  return 0;
}

string
ParseCommandLine(int argc, const char* argv[], utils::Properties& props)
{
  int argindex = 1;
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const string& message) {
        cerr << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else if (strcmp(argv[argindex], "-id") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("clientid", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-d") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("duration", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-w") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("warmup", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-nl") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("no_load", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-ne") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("no_exec", argv[argindex]);
      argindex++;
    } else {
      cerr << "Unknown option " << argv[argindex] << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}

void
UsageMessage(const char* command)
{
  cerr << "Usage: " << command << " [options]" << endl;
  cerr << "Options:" << endl;
  cerr << "  -id n: client id" << endl;
  cerr << "  -P propertyfile: load properties from the given file. Multiple "
          "files can"
       << endl;
  cerr << "                   be specified, and will be processed in the order "
          "specified"
       << endl;
  cerr << "  -d n: test duration in seconds" << endl;
  cerr << "  -w n: warmup/cooldown duration in seconds" << endl;
  cerr << "  -nl: do not load datastore (only executes benchmark)" << endl;
  cerr << "  -ne: do not execute benchmark (only loads datastore)" << endl;
}

inline bool
StrStartWith(const char* str, const char* pre)
{
  return strncmp(str, pre, strlen(pre)) == 0;
}
