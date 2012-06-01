#!/usr/bin/env python
from trial import SharcNetTrial, SharcNetConnection
import sys
from path import path
import yaml

if __name__ == "__main__":
    mcnc = [path(n.strip()) for n in open("./mcnc.txt")]
    uoft = [path(u.strip()) for u in open("./uoft.txt")]
    SharcNetTrial._default_connection = SharcNetConnection()
    nets = mcnc + uoft
    net_info = yaml.load(open('anneal-fast-mean.yml'))

    for net in nets:
        for seed in range(10):
            runtime = 15 + (net_info[net.namebase]['runtime']*3/60) # convert to minutes with a minimum of 15 and factoring in cpu speed differences
            run_count = net_info[net.namebase]['outer iter']
            np = '$BENCHMARK_PATH' + net

            S = SharcNetTrial(params=[('netlist_file', np), ('arch_file', '$BENCHMARK_PATH/k4-1n.xml'), 
                                    ('seed', seed), ('run_count', run_count)], time=runtime, 
                                    priority=1, out_path='$PYVPR_RESULTS/123', 
                                    exe_path='$PYVPR_EXPERIMENTS/freeze_annealer.py')
            #S.make_output_dir()
            #S.write_config()
            #S.submit()
   
    
