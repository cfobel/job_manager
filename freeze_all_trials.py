#!/usr/bin/env python

from __future__ import division
from trial import Trial
import sys
from path import path
import yaml
import shelve
from filter import add_params
import csv
import re

usage=\
"""
    {-uoft -mcnc -all} {-fast -slow} [-test]
    
    -test will only create the first job
"""

if __name__ == "__main__":

    mcnc = [path(n.strip()) for n in open("./mcnc.txt")]
    uoft = [path(u.strip()) for u in open("./uoft.txt")]

    if len(sys.argv) < 3:
        print usage
        exit(0)

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
    else:
        print 'Unknown benchmark type ', sys.agrv[1]
        exit(1)

    if sys.argv[2] == '-fast':
        inner_num = 1
        name += '_fast'
        D = csv.DictReader(open('./runtimes/96_fast_avg.csv'))
    elif sys.argv[2] == '-slow':
        inner_num = 10
        name += '_slow'
        D = csv.DictReader(open('./runtimes/96_slow_avg.csv'))
    else:
        print 'Unknown mode ', sys.argv[2]
    name += '.shelve'

    net_info = dict([(d['netlist'], d) for d in D])

    if len(sys.argv) == 4 and sys.argv[3] == '-test':
        test = True
    else:
        test = False

    trial = shelve.open(name, 'c')
    for net in nets:
        for seed in range(10):
            net_name = re.sub(r'[\.\-]', '_', net.namebase)
            run_count = net_info[net_name]['run_count']
            np = '${BENCHMARK_PATH}/' + net
            params=[('netlist_file', np), ('arch_file', '${BENCHMARK_PATH}/k4-n1.xml'), 
                    ('seed', seed), ('run_count', run_count), ('inner_num', inner_num)]
            add_params(trial, dict(params))
            if test: break
        if test: break
    trial.close()


