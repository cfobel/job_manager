#!/usr/bin/env python
from __future__ import division
from trial import SharcNetTrial, SharcNetConnection
import sys
from path import path
import yaml

if __name__ == "__main__":
    mcnc = [path(n.strip()) for n in open("./mcnc.txt")]
    uoft = [path(u.strip()) for u in open("./uoft.txt")]
    SharcNetTrial._default_connection = SharcNetConnection()
    nets = mcnc + uoft
    net_info = yaml.load(path('/var/www/pyvpr_results/common/'\
            'anneal-fast-mean_outer_iterations_and_runtimes.yml').bytes())

    for net in nets:
        for seed in range(10):
            # Convert runtime to minutes with a minimum of 15 and
            # factoring in cpu speed differences.
            runtime = int(15 + (net_info[net.namebase]['runtime'] * 3 / 60))
            run_count = net_info[net.namebase]['outer iter']
            np = '$BENCHMARK_PATH/' + net

            S = SharcNetTrial(params=[('netlist_file', np), ('arch_file', '$BENCHMARK_PATH/k4-n1.xml'), 
                                    ('seed', seed), ('run_count', run_count), ('inner_num', 1)], time=runtime, 
                                    priority=1, out_path='$PYVPR_RESULTS/96', 
                                    exe_path='$PYVPR_EXPERIMENTS/96/freeze_annealer.py')
            output_path = S.out_path / S.hash_path
            try:
                if '.finished' in S.connection.sftp.listdir(str(output_path)):
                    print '''skipping %s, seed=%d since '.finished' file exists '''\
                            '''in %s''' % (np, seed, output_path)
                    continue
            except IOError:
                # Directory listing failed since output directory
                # doesn't exist yet.
                pass
            S.make_output_dir()
            S.write_config()
            S.submit()
