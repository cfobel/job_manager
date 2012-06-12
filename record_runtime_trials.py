#!/usr/bin/env python
from __future__ import division
from trial import Trial
import sys
from path import path
import shelve
from filter import add_params

if __name__ == "__main__":

    mcnc = [path(n.strip()) for n in open("./mcnc.txt")]
    uoft = [path(u.strip()) for u in open("./uoft.txt")]

    name = '100_trial_'
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

    trial = shelve.open(name, 'c')
    for net in nets:
        np = '${BENCHMARK_PATH}' + net
        params=[('netlist_file', np), 
                ('arch_file', '${BENCHMARK_PATH}k4-n1.xml'), 
                ('inner_num', 10)]
        add_params(trial, dict(params))
    trial.close()
