#!/usr/bin/env python
from __future__ import division
from trial import Trial
import sys
from path import path
import yaml
import shelve
from filter import add_params

if __name__ == "__main__":

    mcnc = [path(n.strip()) for n in open("./mcnc.txt")]
    uoft = [path(u.strip()) for u in open("./uoft.txt")]

    name = '96_trial_'
    if sys.argv[1] == '-mcnc':    
        nets = mcnc
        name += 'mcnc'
    elif sys.argv[1] == '-uoft':
        nets = uoft
        name += 'uoft'
    elif sys.argv[1] == '-all':
        nets = mcnc + uoft
        name += 'all'
    name += '.shelve'

    net_info = yaml.load(path('./'\
            'anneal-fast-mean_outer_iterations.yml').bytes())

    trial = shelve.open(name, 'c')
    i=0
    for net in nets:
        for seed in range(10):
            i += 1
            run_count = net_info[net.namebase]['outer iter']
            np = '${BENCHMARK_PATH}' + net
            params=[('netlist_file', np), ('arch_file', '${BENCHMARK_PATH}k4-n1.xml'), 
                    ('seed', seed), ('run_count', run_count), ('inner_num', 1)]
            add_params(trial, dict(params))
    print i
    trial.close()
