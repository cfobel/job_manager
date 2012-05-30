#!/usr/bin/env python

import sys
import yaml
from resolve_vars import resolve_path
from path import path
from trial import SharcNetTrial, CoalitionTrial, CoalitionConnection, SharcNetConnection


help_string = """
    Runs an annealer on  a server.
    
    vpr_anneal <netlist path> <arch path> <output path>  <--coalition | --sharcnet>
"""

if __name__ == "__main__":
    
    if len(sys.argv) != 5:
        print help_string
        exit(1)

    #look in this machines environment and eval path variables
#    env = open(path(__file__).parent / path('environments/env_vars.yml'))
#    env = yaml.load(env)
    net_path = sys.argv[1] #resolve_path(env, sys.argv[1])
    arch_path = sys.argv[2] #resolve_path(env, sys.argv[2])
    output = sys.argv[3] #resolve_path(env, sys.argv[3])
    server = sys.argv[4][2:]

    # create the tuple of positional arguments and relative path to executable
    prog = 'python $PYVPR_EXPERIMENTS/77/sa_x_ga.py'

    params = dict()
    params['netlist_files'] = net_path
    params['arch_file'] = arch_path
    params['scaling_factor'] = 0.0 
    params['o'] = output

    # submit the job to the server
    if server == 'coalition':
        CoalitionTrial._default_connection = CoalitionConnection()
        T = CoalitionTrial(params=params, time=5, priority=7, exe_path=prog, out_path=output )
    elif server == 'sharcnet':
        SharcNetTrial._default_connection = SharcNetConnection()
        T = SharcNetTrial(params=params, time=5, priority=7, exe_path=prog, out_path=output)

    T.make_output_dir()
    T.write_config()
    print T.submit()

