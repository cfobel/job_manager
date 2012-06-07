#!/usr/bin/env python
from __future__ import division
from trial import SharcNetTrial, SharcNetConnection
import sys
from path import path
import yaml
import shelve
from filter import add_params

if __name__ == "__main__":

    if sys.argv[1] == '-gen':
        mcnc = [path(n.strip()) for n in open("./mcnc.txt")]
        uoft = [path(u.strip()) for u in open("./uoft.txt")]
        nets = mcnc + uoft
        net_info = yaml.load(path('/var/www/pyvpr_results/common/'\
                'anneal-fast-mean_outer_iterations_and_runtimes.yml').bytes())
        trial = shelve.open('96_trial_file.shelve', 'c')

        for net in nets:
            for seed in range(10):
                # Convert runtime to minutes with a minimum of 15 and
                # factoring in cpu speed differences.
                runtime = int(15 + (net_info[net.namebase]['runtime'] * 3 / 60))
                run_count = net_info[net.namebase]['outer iter']
                np = '$BENCHMARK_PATH/' + net
                params=[('netlist_file', np), ('arch_file', '$BENCHMARK_PATH/k4-n1.xml'), 
                        ('seed', seed), ('run_count', run_count), ('inner_num', 1)]
                add_params(trial, dict(params))
        trial.close()
