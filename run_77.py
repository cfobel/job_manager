#!/usr/bin/env python
from trial import SharcNetTrial, SharcNetConnection
import sys
from path import path


if __name__ == "__main__":
    nets = open("./mcnc.txt")
    SharcNetTrial._default_connection = SharcNetConnection()

    for net in nets:
        np = path('$BENCHMARK_PATH') / path(net.strip())
        print np
        S = SharcNetTrial(params=[('netlist_file', np), ('arch_file', '$BENCHMARK_PATH/k4-n1.xml'), 
                                    ('scaling_factor', '0.0'), ('stride_factor', '0.0')], time=25, 
                                    priority=1, out_path='$PYVPR_RESULTS/',
                                    exe_path='$PYVPR_EXPERIMENTS/77/sa_x_ga.py')
        S.make_output_dir()
        S.write_config()
        S.submit()
