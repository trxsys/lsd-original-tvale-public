from ConfigParser import RawConfigParser
from fabric.api import task, run, env, roles, parallel, execute
from fabric.context_managers import cd, settings
from multiprocessing import Value
from subprocess import call

################################################################################
# constants
config_file = 'config.ini'
code_dir = 'lsd'
results_dir = 'lsd-data'
tpcc_dir = 'bench/tpcc/pytpcc'
make_config = 'debug' # 'debug', 'release'
make_threads = 1
dtach_cmd  = 'dtach -n `mktemp -u /tmp/lsd.XXXXXXX.dtach` '
dtach_cmd += env.shell
dtach_cmd += " '{}'"
ld_lib_path = 'LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.1 LD_LIBRARY_PATH=lib/thrift-0.9.3/lib:lib '
setup = 'di' # 'di', 'aws'

################################################################################
# cluster di vs. amazon aws
if setup == 'di':
  code_path_src = '/home/tvale/phd'
  code_path_dst = '/localhome/tvale/lsd-experiments'
elif setup == 'aws':
  code_path_src = '/home/ubuntu'
  code_path_dst = '/home/ubuntu/lsd-experiments'
  ssh_key = '/home/ubuntu/.ssh/aws-tvale-eu-central-1.pem'
else:
  raise NotImplementedError

################################################################################
# parse config
config = RawConfigParser()
config.read(config_file)
barrier_host = config.get('barrier', 'host')
barrier_port = config.get('barrier', 'port')
ycsb_clients = config.get('ycsb', 'instances').split()
ycsb_client_hosts = []
ycsb_client_threads = []
for c in ycsb_clients:
  host, threads = c.split(':')
  ycsb_client_hosts.append(host)
  ycsb_client_threads.append(threads)
tpcc_clients = config.get('tpcc', 'clients').split()
tpcc_client_hosts = []
for c in tpcc_clients:
  host, threads = c.split(':')
  tpcc_client_hosts.append(host)
tpcc_coord_host = config.get('tpcc', 'coord')
servers = config.get('server', 'instances').split()
server_hosts = []
server_ports = []
for s in servers:
  host, port = s.split(':')
  server_hosts.append(host)
  server_ports.append(port)

################################################################################
# fabric roles
env.roledefs['ycsb'] = ycsb_client_hosts
env.roledefs['tpcc_coord'] = [tpcc_coord_host]
env.roledefs['tpcc_client'] = tpcc_client_hosts
env.roledefs['server'] = server_hosts
env.roledefs['barrier'] = [barrier_host]

################################################################################
# globals
env.ycsb_clients = ycsb_client_hosts
env.ycsb_threads = ycsb_client_threads
env.tpcc_clients = tpcc_client_hosts
env.tpcc_coord = tpcc_coord_host
env.servers = server_hosts
env.server_ports = server_ports

################################################################################
# prepare
@task
def prepare(config = make_config, threads = make_threads):
  execute(create_path)
  execute(copy_code)
  execute(make_clean)
  execute(compile_thrift)
  execute(compile_protobuf)
  execute(compile_server, config, threads)
  execute(compile_ycsb, 'ycsb', config, threads)
  execute(compile_ycsb, 'incr', config, threads)
  execute(compile_ycsb, 'hotk', config, threads)
  execute(compile_asrt, config, threads)
  execute(compile_barrier, config, threads)

@task
@roles('ycsb', 'tpcc_coord', 'tpcc_client', 'barrier', 'server')
@parallel
def create_path():
  run('mkdir -p {}'.format(code_path_dst))
  run('mkdir -p {}/{}'.format(code_path_dst, results_dir))

@task
def copy_code():
  hosts = set(ycsb_client_hosts + [ tpcc_coord_host ] + tpcc_client_hosts
              + server_hosts + [ barrier_host ])
  for host in hosts:
    if setup == 'di':
      cmd  = ['rsync']
      cmd += ['-a']
      cmd += ['-v']
      cmd += ['{}/{}'.format(code_path_src, code_dir)]
      cmd += ['{}:{}'.format(host, code_path_dst)]
    elif setup == 'aws':
      cmd  = ['scp']
      cmd += ['-i', '{}'.format(ssh_key)]
      cmd += ['-r']
      cmd += ['{}/{}'.format(code_path_src, code_dir)]
      cmd += ['{}:{}'.format(host, code_path_dst)]
    else:
      raise NotImplementedError
    call(cmd)

@task
@roles('ycsb', 'tpcc_coord', 'tpcc_client', 'barrier', 'server')
@parallel
def make_clean():
  with cd('{}/{}'.format(code_path_dst, code_dir)):
    run('make clean')

@task
@roles('ycsb', 'tpcc_coord', 'tpcc_client', 'barrier', 'server')
@parallel
def compile_thrift():
  with cd('{}/{}'.format(code_path_dst, code_dir)):
    run('./thrift/compile.sh')

@task
@roles('ycsb', 'tpcc_coord', 'tpcc_client', 'barrier', 'server')
@parallel
def compile_protobuf():
  with cd('{}/{}'.format(code_path_dst, code_dir)):
    run('./protobuf/compile.sh')

@task
@roles('server')
@parallel
def compile_server(config = make_config, threads = make_threads):
  with cd('{}/{}'.format(code_path_dst, code_dir)):
    run('./scripts/compile.sh dist/server-occ {} {}'.format(config, threads))
    run('./scripts/compile.sh dist/server-occ-woundwait {} {}'.format(config,
                                                                      threads))
    run('./scripts/compile.sh dist/server-occ-waitdie {} {}'.format(config,
                                                                    threads))
    run('./scripts/compile.sh dist/server-occ-nowait {} {}'.format(config,
                                                                   threads))
    run('./scripts/compile.sh dist/server-2pl {} {}'.format(config, threads))
    run('./scripts/compile.sh dist/server-2pl-woundwait {} {}'.format(config,
                                                                      threads))
    run('./scripts/compile.sh dist/server-2pl-waitdie {} {}'.format(config,
                                                                    threads))
    run('./scripts/compile.sh dist/server-2pl-nowait {} {}'.format(config,
                                                                   threads))

@task
@roles('ycsb')
@parallel
def compile_ycsb(bench = 'ycsb', config = make_config, threads = make_threads):
  with cd('{}/{}'.format(code_path_dst, code_dir)):
    cmd = './scripts/compile.sh dist/{}-std-occ {} {}'
    run(cmd.format(bench, config, threads))
    cmd = './scripts/compile.sh dist/{}-std-occ-2pcseq {} {}'
    run(cmd.format(bench, config, threads))
    cmd = './scripts/compile.sh dist/{}-std-2pl {} {}'
    run(cmd.format(bench, config, threads))
    cmd = './scripts/compile.sh dist/{}-lsd-occ {} {}'
    run(cmd.format(bench, config, threads))
    cmd = './scripts/compile.sh dist/{}-lsd-occ-2pcseq {} {}'
    run(cmd.format(bench, config, threads))
    cmd = './scripts/compile.sh dist/{}-lsd-2pl {} {}'
    run(cmd.format(bench, config, threads))

@task
@roles('ycsb')
@parallel
def compile_asrt(config = make_config, threads = make_threads):
  with cd('{}/{}'.format(code_path_dst, code_dir)):
    cmd = './scripts/compile.sh dist/asrt-std-occ {} {}'
    run(cmd.format(config, threads))
    cmd = './scripts/compile.sh dist/asrt-std-occ-2pcseq {} {}'
    run(cmd.format(config, threads))
    cmd = './scripts/compile.sh dist/asrt-std-2pl {} {}'
    run(cmd.format(config, threads))
    cmd = './scripts/compile.sh dist/asrt-lsd-occ {} {}'
    run(cmd.format(config, threads))
    cmd = './scripts/compile.sh dist/asrt-lsd-occ-assume {} {}'
    run(cmd.format(config, threads))
    cmd = './scripts/compile.sh dist/asrt-lsd-occ-2pcseq {} {}'
    run(cmd.format(config, threads))
    cmd = './scripts/compile.sh dist/asrt-lsd-occ-2pcseq-assume {} {}'
    run(cmd.format(config, threads))
    cmd = './scripts/compile.sh dist/asrt-lsd-2pl {} {}'
    run(cmd.format(config, threads))

@task
@roles('barrier')
@parallel
def compile_barrier(config = make_config, threads = make_threads):
  with cd('{}/{}'.format(code_path_dst, code_dir)):
    run('./scripts/compile.sh dist/barrier {} {}'.format(config, threads))

@task
def copy_config():
  hosts = set(ycsb_client_hosts + [tpcc_coord_host] + tpcc_client_hosts
              + server_hosts + [ barrier_host ])
  for host in hosts:
    if setup == 'di':
      cmd  = ['rsync']
      cmd += ['-a']
      cmd += ['-v']
      cmd += ['{}/{}/{}'.format(code_path_src, code_dir, config_file)]
      cmd += ['{}:{}/{}'.format(host, code_path_dst, code_dir)]
      call(cmd)
    elif setup == 'aws':
      raise NotImplementedError
    else:
      raise NotImplementedError

################################################################################
# tpcc
@task
def tpcc(ccontrol = "occ", lsd = "no", twopc_seq = "no", duration="30",
         warmup = "8", warehouse = "1"):
  if lsd in ("Yes", "yes", "y", "Y", "1", "true", "True"):
    lsd = True
  else:
    lsd = False
  if twopc_seq in ("Yes", "yes", "y", "Y", "1", "true", "True"):
    twopc_seq = True
  else:
    twopc_seq = False
  assert ccontrol == "occ" or ccontrol == "2pl"
  assert not twopc_seq or ccontrol == "occ"
  duration = int(duration)
  warmup = int(warmup)
  execute(tpcc_coord, duration, warmup, warehouse, ccontrol, lsd, twopc_seq)
  execute(kill_server)

@roles('tpcc_coord')
def tpcc_coord(duration, warmup, warehouse, ccontrol, lsd, twopc_seq):
  if lsd:
    suffix = '-lsd'
  else:
    suffix = '-std'
  suffix += '-' + ccontrol
  if twopc_seq:
    suffix += '-2pcseq'
  coord_cmd  = 'python coordinator.py --config config.ini '
  coord_cmd += '--duration {0} --warmup {1} --nurand tpcc_nurand.bin '
  coord_cmd += '--no-load --warehouses {2} lsd '
  coord_cmd += '>w{2}.tpcc{3}.out.txt 2>w{2}.tpcc{3}.err.txt'
  coord_cmd  = coord_cmd.format(duration, warmup, warehouse, suffix)
  with cd('{}/{}/{}'.format(code_path_dst, code_dir, tpcc_dir)):
    run(coord_cmd)

@task
@roles('tpcc_coord')
def tpcc_load(suffix = 'w1'):
  fname = 'tpcc_{}'.format(suffix)
  cmd = 'python -u scripts/load_from_disk.py {}'.format(fname)
  with cd('{}/{}'.format(code_path_dst, code_dir)):
    run(cmd)

@task
@roles('server')
@parallel
def tpcc_load_rocksdb(warehouses = '1', sharding = 'tpcc'):
  host = run('hostname')
  for sid, shost in enumerate(env.servers):
    if shost == host:
      id = sid
      break
  nservers = len(env.servers)
  cmd_rm = 'rm -r rocks_db_{}_{}'
  cmd_tar = 'tar -xf rocks_db_{}_w{}_s{}_{}-{}.tar.xz'
  cmd_tar = cmd_tar.format(str(id), warehouses, sharding, str(id + 1),
                           str(nservers))
  with cd('{}/{}'.format(code_path_dst, code_dir)):
    with settings(warn_only = True):
      for table in range(0, 12):
        run(cmd_rm.format(str(id), table))
    run(cmd_tar)

@task
@roles('tpcc_coord')
def tpcc_copy_output(run_number, warehouse, threads, clients, suffix_src,
                     suffix_dst, sharding, storage):
  with cd('{}'.format(code_path_dst)):
    cmd = 'mkdir -p {}'
    cmd = cmd.format(results_dir)
    run(cmd)
    src_file = 'w{}.tpcc{}.out.txt'
    src_file = src_file.format(warehouse, suffix_src)
    dst_file = 'w{}.tpcc-s{}.{}.{}.srv{}.clt{}.thr{}.run{}.out.txt'
    dst_file = dst_file.format(warehouse, sharding, storage, suffix_dst,
                               len(env.servers), clients, threads, run_number)
    cmd = 'mv {}/{}/{} {}/{}'
    cmd = cmd.format(code_dir, tpcc_dir, src_file, results_dir, dst_file)
    run(cmd)
    src_file = 'w{}.tpcc{}.err.txt'
    src_file = src_file.format(warehouse, suffix_src)
    dst_file = 'w{}.tpcc-s{}.{}.{}.srv{}.clt{}.thr{}.run{}.err.txt'
    dst_file = dst_file.format(warehouse, sharding, storage, suffix_dst,
                               len(env.servers), clients, threads, run_number)
    cmd = 'mv {}/{}/{} {}/{}'
    cmd = cmd.format(code_dir, tpcc_dir, src_file, results_dir, dst_file)
    run(cmd)

################################################################################
# ycsb
@task
def ycsb(bench = 'ycsb', ccontrol = "occ", lsd = "no", multiget = "no",
         twopc_seq = "no", assume = "no", duration="30", warmup = "15",
         workload = "workloadb-k16777216-s3"):
  if lsd in ("Yes", "yes", "y", "Y", "1", "true", "True"):
    lsd = True
  else:
    lsd = False
  if multiget in ("Yes", "yes", "y", "Y", "1", "true", "True"):
    multiget = True
  else:
    multiget = False
  if twopc_seq in ("Yes", "yes", "y", "Y", "1", "true", "True"):
    twopc_seq = True
  else:
    twopc_seq = False
  if assume in ("Yes", "yes", "y", "Y", "1", "true", "True"):
    assume = True
  else:
    assume = False
  assert ccontrol == "occ" or ccontrol == "2pl"
  assert not twopc_seq or ccontrol == "occ"
  if assume:
    assert bench == 'asrt'
  duration = int(duration)
  warmup = int(warmup)
  execute(ycsb_client, bench, ccontrol, lsd, multiget, twopc_seq, assume,
          duration, warmup, workload)
  execute(kill_server)

@roles('ycsb')
@parallel
def ycsb_client(bench, ccontrol, lsd, multiget, twopc_seq, assume, duration,
                warmup, workload):
  host = run('hostname')
  for cid, chost in enumerate(env.ycsb_clients):
    if chost == host:
      id = cid
      break
  exe = bench
  if lsd:
    exe += '-lsd'
  else:
    exe += '-std'
  if multiget:
    exe += '-mg'
  exe += '-' + ccontrol
  if twopc_seq:
    exe += '-2pcseq'
  if assume:
    exe += '-assume'
  client_cmd  = ld_lib_path
  client_cmd += './dist/{0} -id {1} -d {2} -w {3} -nl 1 '
  client_cmd += '-P bench/ycsb/workloads/{4}.spec '
  client_cmd += '>{4}.{0}.{1}.out.txt 2>{4}.{0}.{1}.err.txt'
  client_cmd  = client_cmd.format(exe, id, duration, warmup, workload)
  with cd('{}/{}'.format(code_path_dst, code_dir)):
    run(client_cmd)

@task
@roles('ycsb')
@parallel
def ycsb_copy_output(bench, run_number, workload, suffix_src, suffix_dst,
                     storage):
  host = run('hostname')
  for cid, chost in enumerate(env.ycsb_clients):
    if chost == host:
      id = cid
      break
  threads = env.ycsb_threads[id]
  clients = len(env.ycsb_clients)
  with cd('{}'.format(code_path_dst)):
    cmd = 'mkdir -p {}'
    cmd = cmd.format(results_dir)
    run(cmd)
    src_file = '{}.{}{}.{}.out.txt'
    src_file = src_file.format(workload, bench, suffix_src, id)
    dst_file = '{}.{}{}.{}.srv{}.clt{}-{}.thr{}.run{}.out.txt'
    dst_file = dst_file.format(workload, bench, suffix_dst, storage,
                               len(env.servers), id, clients, threads,
                               run_number)
    cmd = 'mv {}/{} {}/{}'
    cmd = cmd.format(code_dir, src_file, results_dir, dst_file)
    run(cmd)
    src_file = '{}.{}{}.{}.err.txt'
    src_file = src_file.format(workload, bench, suffix_src, id)
    dst_file = '{}.{}{}.{}.srv{}.clt{}-{}.thr{}.run{}.err.txt'
    dst_file = dst_file.format(workload, bench, suffix_dst, storage,
                               len(env.servers), id, clients, threads,
                               run_number)
    cmd = 'mv {}/{} {}/{}'
    cmd = cmd.format(code_dir, src_file, results_dir, dst_file)
    run(cmd)

@task
@roles('ycsb')
@parallel
def ycsb_load(suffix = 'k16777216'):
  raise NotImplementedError
  # host = run('hostname')
  # for cid, chost in enumerate(env.ycsb_clients):
  #   if chost == host:
  #     id = cid
  #     break
  # if id == 0:
  #   fname = 'ycsb_{}'.format(suffix)
  #   cmd = 'python -u scripts/load_from_disk.py {}'.format(fname)
  #   with cd('{}/{}'.format(code_path_dst, code_dir)):
  #     run(cmd)

@task
@roles('server')
@parallel
def ycsb_load_rocksdb(bench = 'ycsb', suffix = 'k16777216'):
  host = run('hostname')
  for sid, shost in enumerate(env.servers):
    if shost == host:
      id = sid
      break
  nservers = len(env.servers)
  cmd_rm = 'rm -r rocks_db_{}_{}'
  cmd_tar = 'tar -xf rocks_db_{}_{}_{}_{}-{}.tar.xz'
  cmd_tar = cmd_tar.format(str(id), bench, suffix, str(id + 1),
                           str(nservers))
  with cd('{}/{}'.format(code_path_dst, code_dir)):
    with settings(warn_only = True):
      for table in range(0, 12):
        run(cmd_rm.format(str(id), table))
    run(cmd_tar)

################################################################################
# barrier
@roles('barrier')
def barrier():
  barrier_cmd  = ld_lib_path
  barrier_cmd += './dist/barrier >barrier.out.txt 2>barrier.err.txt'
  with cd('{}/{}'.format(code_path_dst, code_dir)):
    run(dtach_cmd.format(barrier_cmd))

@task
@roles('barrier')
def kill_barrier():
  if setup == 'di':
    run('killall --user tvale --signal KILL barrier')
  elif setup == 'aws':
    raise NotImplementedError
  else:
    raise NotImplementedError

################################################################################
# server
@task
@roles('server')
@parallel
def server(suffix = 'occ'):
  host = run('hostname')
  for sid, shost in enumerate(env.servers):
    if shost == host:
      id = sid
      break
  exe = 'server-{}'.format(suffix)
  server_cmd  = 'ulimit -n 32768; ulimit -c unlimited; '
  server_cmd += ld_lib_path
  server_cmd += './dist/{0} {1} >{0}.out.{1}.txt 2>{0}.err.{1}.txt'
  server_cmd  = server_cmd.format(exe, id)
  with cd('{}/{}'.format(code_path_dst, code_dir)):
    run(dtach_cmd.format(server_cmd))

@task
@roles('server')
@parallel
def kill_server():
  with settings(warn_only = True):
    with cd('{}/{}'.format(code_path_dst, code_dir)):
      if setup == 'di':
        run('killall --user tvale --signal KILL server-occ')
        run('killall --user tvale --signal KILL server-occ-woundwait')
        run('killall --user tvale --signal KILL server-occ-waitdie')
        run('killall --user tvale --signal KILL server-occ-nowait')
        run('killall --user tvale --signal KILL server-2pl')
        run('killall --user tvale --signal KILL server-2pl-woundwait')
        run('killall --user tvale --signal KILL server-2pl-waitdie')
        run('killall --user tvale --signal KILL server-2pl-nowait')
      elif setup == 'aws':
        raise NotImplementedError
      else:
        raise NotImplementedError
