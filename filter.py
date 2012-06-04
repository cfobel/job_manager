#!/usr/bin/env python

from trial import CoalitionTrial, SharcNetTrial
import shelve
from path import path

def _parse_args():
    """Parses arguments, returns ``(options, args)``."""
    from argparse import ArgumentParser

    parser = ArgumentParser(description="""\
    Submits all unsubmitted jobs to coalition or sharcnet
    usage: ./filter <coalition / sharcnet> <path to program>"""
    """ <path to parameter file> <result name>
    eg.    filter coalition /charcoal/resound.py ./data.pickle ./issue_100.""",
                            epilog="""\
(C) 2012 Ryan Pattison and Christian Fobel, licensed under the terms of GPL2""",
                           )
    parser.add_argument('-coalition', action='store_true')
    parser.add_argument('-sharcnet', action='store_true')
    parser.add_argument('-submit', action='store_true')
    parser.add_argument('-update', action='store_true')
    
    parser.add_argument('-run_time', nargs=1,
                         dest='run_time', type=int)
    parser.add_argument('-priority', nargs=1,
                        dest='priority', type=int)
    parser.add_argument('-param_file', required=True, nargs=1, 
                        dest='param_file', type=path)
    parser.add_argument('-script', required=True, nargs=1,
                        dest='script', type=path)
    parser.add_argument('-trial_name', required=True, nargs=1,
                         dest='trial_name', type=str )
   
    args = parser.parse_args()
    if args.submit:
        args.run_time = args.run_time[0]
        args.priority = args.priority[0]
    args.param_file = args.param_file[0]
    args.script = args.script[0]
    args.trial_name = args.trial_name[0]
    
    return args


def update(param_file, exe_path, out_path):
    entry = shelve.open(param_file)
    for k, v in entry.iteritems():
        state = dict(v)
        if state['state'] != 'submitted':
            continue
        if state['queue'] == 'sharcnet':
            T = SharcNetTrial(params=eval(k), exe_path=exe_path, out_path=out_path)
        elif state['queue'] == 'coalition':
            T = CoalitionTrial(params=eval(k), exe_path=exe_path, out_path=out_path)
        else:
            print 'Unknown Queue ', state['queue'] 
            continue
        new_state = T.get_state()
        if not new_state:
            continue
        else:
            state['state'] = new_state
    entry.close()


def add_params(fname, params):
    p = []
    for k,v in params.iteritems():
        p.append((k,v))
    p = sorted( p )
    d = shelve.open(fname)
    d[str(tuple(p))]= {'state':'waiting', 'queue':None, 'id':None}
    d.close()

def run_coalition(trial_file, prog_path, result_path, run_time, priority):
     run_parameters(trial_file, lambda x: False, lambda x: True, prog_path,
                    result_path, run_time, priority)


def run_sharcnet(trial_file, prog_path, result_path, run_time, priority):
    run_parameters(trial_file, lambda x: True, lambda x: False, prog_path, 
                    result_path, run_time, priority)


def run_parameters(trial_file, sharc_filter, coalition_filter, prog_path,
                    result_path, run_time, priority):

    trial =  shelve.open(trial_file)
 
    parameters = trial.keys()
    for p in parameters:
        if trial[p]['state'] == 'waiting':
            assert(trial[p]['queue'] == None)
            assert(trial[p]['id'] == None)
            if sharc_filter(p):
                trial[p]['state'] = 'submitted'
                trial[p]['queue'] = 'sharcnet'                
                T = SharcNetTrial(out_path=result_path, 
                                    exe_path=prog_path, 
                                    params=eval(p), 
                                    time=run_time, 
                                    priority=priority)
            elif coalition_filter(p):
                trial[p]['state'] = 'submitted'
                trial[p]['queue'] = 'coalition'
                T = CoalitionTrial(out_path=results_path, 
                                    exe_path=prog_path, 
                                    params=eval(p), 
                                    time=run_time, 
                                    priority=priority)
            else:
                continue

            T.create_output_dir()
            T.write_config_file()
            T.submit()
            trial[p]['id'] = T.get_id()

    trial.close()

if __name__ == "__main__":
    args = _parse_args()
    if args.submit:
        if args.coalition:
            run_coalition(args.param_file, args.script, args.trial_name, args.run_time, args.priority)
        elif args.sharcnet: 
            run_sharcnet(args.param_file, args.script, args.trial_name, args.run_time, args.priority)
        else:
            print 'No server Specified; -coalition or -sharcnet'
    elif args.update:
            update(args.param_file, args.script, args.trial_name)
    else:
        print 'No action; use -update or -submit.'
