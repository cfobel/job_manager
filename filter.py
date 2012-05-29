#!/usr/bin/env python

import sys
from trial import CoalitionTrial, SharcNetTrial
import shelve


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
    parser.add_argument('-param_file', required=True, nargs=1, 
                        dest='param_file', type=path)
    parser.add_argument('-script', required=True, nargs=1,
                        dest='script', type=path)
    parser.add_argument('-trial_name', required=True, nargs=1,
                         dest='trial_name', type=str )
    
    args = parser.parse_args()
    
    args.param_file = args.param_file[0]
    args.script = args.script[0]
    args.trial_name = args.trial_name[0]
    
    return args


def parameters(fname):
    return shelve.open(fname, 'w')


def add_params(self, **params):
    params = []
    for k,v in params.iteritems():
        params.append((k,v))
    params = sorted( params )
    d = parameters()
    d[str(tuple(params))]= {'state':'waiting', 'queue':None, 'id':None}


def run_coalition(trial_file, prog_path, result_path):
     run_parameters(trial_file, lambda x: False, lambda x: True, prog_path,
                    result_path)   


def run_sharcnet(trial_file, prog_path, result_path):
    run_parameters(trial_file, lambda x: True, lambda x: False, prog_path, 
                    result_path)


def run_parameters(trial_file, sharc_filter, coalition_filter, prog_path,
                    result_path):

    trial =  shelve.open(trial_file)
 
    parameters = trial.keys()
    for p in parameters:
        if trial[p]['state'] == 'waiting':
            assert(trial[p]['queue'] == None)
            assert(trial[p]['id'] == None)
            if sharc_filter(p):
                trial[p]['state'] = 'submitted'
                trial[p]['queue'] = 'sharcnet'                
                T = SharcNetTrial(result_path, prog_path, eval(p))
            elif coalition_filter(p):
                trial[p]['state'] = 'submitted'
                trial[p]['queue'] = 'coalition'
                T = CoalitionTrial(results_path, prog_path, eval(p))
            else:
                continue

            T.create_output_dir()
            T.write_config_file()
            T.submit()
            trial[p]['id'] = T.get_id()

    trial.close()

if __name__ == "__main__":
    args - _parse_args()
    if args.coalition:
        run_coalition(args.param_file, args.script, args.trial_name)
    elif args.sharcnet: 
        run_sharcnet(args.param_file, args.script, args.trial_name)
    else:
        print 'unknown server'
