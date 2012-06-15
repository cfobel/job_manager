#!/usr/bin/env python

from trial import CoalitionTrial, SharcNetTrial, BaseTrial, Trial, SharcNetConnection
import shelve
from path import path
import yaml


def submit_all(trial_file, trial_list):
    trial = shelve.open(args.param_file, writeback=True)
    for T, p in trial_list:
        T.make_output_dir()
        T.write_config()
        T.submit()
        trial[p][Trial.ID] = T.get_id()
    trial.close()


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
    parser.add_argument('-local', action='store_true')
    parser.add_argument('-verbose', action='store_true')
    parser.add_argument('-submit', action='store_true')
    parser.add_argument('-update', action='store_true')
    parser.add_argument('-rehash', action='store_true')
    parser.add_argument('-show', action='store_true')
    parser.add_argument('--test', action='store_true')
    parser.add_argument('-state', nargs='+', type=str)
    parser.add_argument('-run_time', nargs=1,
                         dest='run_time', type=int)
    parser.add_argument('-priority', nargs=1,
                        dest='priority', type=int)
    parser.add_argument('-param_file', required=True, nargs=1, 
                        dest='param_file', type=path)
    parser.add_argument('-script', nargs=1,
                        dest='script', type=path)
    parser.add_argument('-trial_name', nargs=1,
                         dest='trial_name', type=str )
   
    args = parser.parse_args()
    if args.submit:
        args.run_time = args.run_time[0]
        args.priority = args.priority[0]
    args.param_file = args.param_file[0]
    if args.script:
        args.script = args.script[0]
    if args.trial_name:
        args.trial_name = args.trial_name[0]
    
    return args


def rehash(exe_path, out_path, server):
    if server == Trial.SHARCNET:
        T = SharcNetTrial(params=[], exe_path=exe_path, out_path=out_path)
    elif server == Trial.COALITION:
        T = CoalitionTrial(params=[], exe_path=exe_path, out_path=out_path)
    else:
        print 'unknown server ', server
        return

    for folder in T.connection.listdir(T.out_path):
        config = path(folder) / path('config.yml')
        files = T.connection.listdir(T.out_path / path(folder))
        if '.finished' not in files:
            print 'Script not done'
            continue
        try:
            cfile = T.connection.open(T.out_path / config)
        except:
            print "couldn't open:", T.out_path / config
            continue
        data = yaml.load(cfile)
        sort_params = sorted(data[1])
        new_path = T.out_path / T._hash(data[0], sort_params)
        cfile.close()
        if new_path != T.out_path / folder:
            # rewrite the sorted parameters into the config file
            cfile = T.connection.open(T.out_path / config, 'w')
            cfile.write(yaml.dump(data))
            cfile.close()
            T.connection.rename(T.out_path / folder, new_path)
        else:
            print folder, ' already in right spot'


def update(param_file, exe_path, out_path):
    entry = shelve.open(param_file, writeback=True)
    for k, v in entry.iteritems():
        if v[Trial.STATE] != Trial.SUBMITTED:
            print 'state ', v[Trial.STATE]
            continue
        if v[Trial.QUEUE] == Trial.SHARCNET:
            T = SharcNetTrial(params=eval(k), exe_path=exe_path, 
                                            out_path=out_path)
        elif v[Trial.QUEUE] == Trial.COALITION:
            T = CoalitionTrial(params=eval(k), exe_path=exe_path, 
                                                out_path=out_path)
        else:
            print 'Unknown Queue ', v[Trial.QUEUE] 
            continue
        new_state = T.get_state()
        if not new_state:
            print 'no update'
            continue
        else:
            v[Trial.STATE] = new_state
            entry[k] = v
    entry.close()


# Use this as a one time run.
def test(x):
    if hasattr(test, '_ran'):
        return False
    else:
        test._ran = True
        return False


def add_params(trial_file, params):
    if isinstance( params, dict):
        p = sorted([(k, v) for k,v in params.iteritems()])
    else:
        p = sorted(params)    
    trial_file[str(tuple(p))]= {Trial.STATE:Trial.WAITING, 
                                Trial.QUEUE:None, 
                                Trial.ID:None}


def run_coalition(trial_file, prog_path, result_path, run_time, priority,
                                                             verbose=False):
    return run_parameters(trial_file, lambda x: False, lambda x: True, 
                prog_path, result_path, run_time, priority, verbose=verbose)


def run_sharcnet(trial_file, prog_path, result_path, run_time, priority, 
                                                            verbose=False):
    return run_parameters(trial_file, lambda x: True, lambda x: False, 
            prog_path, result_path, run_time, priority, verbose=verbose)


def run_parameters(trial_file, sharc_filter, coalition_filter, prog_path,
                    result_path, run_time, priority, verbose=False, test=False):

    trial =  shelve.open(trial_file, writeback=True)
    trial_objs = list()
    parameters = trial.keys()
    for p in parameters:
        if trial[p][Trial.STATE] == Trial.WAITING:
            assert(trial[p][Trial.QUEUE] == None)
            assert(trial[p][Trial.ID] == None)
            if sharc_filter(p):
                trial[p][Trial.STATE] = Trial.SUBMITTED
                trial[p][Trial.QUEUE] = Trial.SHARCNET

                if SharcNetTrial._default_connection == None:
                    C = SharcNetConnection(username='cfobel')
                    SharcNetTrial._default_connection = C 
               
                T = SharcNetTrial(out_path=result_path,
                                    exe_path=prog_path, 
                                    params=eval(p), 
                                    time=run_time, 
                                    priority=priority,
                                    verbose=verbose,
                                    test=test)

            elif coalition_filter(p):
                trial[p][Trial.STATE] = Trial.SUBMITTED
                trial[p][Trial.QUEUE] = Trial.COALITION
                T = CoalitionTrial(out_path=result_path, 
                                    exe_path=prog_path, 
                                    params=eval(p), 
                                    time=run_time, 
                                    priority=priority,
                                    verbose=verbose,
                                    test=test)
            else:
                continue
            trial_objs.append((T, p))
    trial.close()
    return trial_objs


def test_coalition(trial_file, script='python '+Trial.EXPERIMENTS/'test.py',
                                                 output=Trial.RESULTS/'test'):
    submit_all(trial_file, run_parameters(trial_file, lambda x: False, test,
                                script, output, 3, 1, 
                                verbose=True, test=True))


def test_sharcnet(trial_file, script='python '+Trial.EXPERIMENTS/'test.py', 
                                                output=Trial.RESULTS/'test'):
    submit_all(trial_file, run_parameters(trial_file, test, lambda x: False,                                 
                                script, output, 3, 1, 
                                verbose=True, test=True))


def show(param_file, state_var, value):
    entry = shelve.open(param_file)
    for k, v in entry.iteritems():
        if value == None:
            print v[str(state_var)]
        elif str(v[str(state_var)]) == str(value):
            print k, v
    entry.close()


if __name__ == "__main__":
    args = _parse_args()
    if args.verbose:
        print 'filter args := ', args
    if args.show:
        value = None
        if len(args.state) > 1:
            value = args.state[1]
        show(args.param_file, args.state[0], value)
    elif args.submit:
        if args.coalition:
            submit_all(args.param_file, run_coalition(args.param_file,
                                args.script, args.trial_name, 
                                args.run_time, args.priority, 
                                verbose=args.verbose))
        elif args.sharcnet: 
            submit_all(args.param_file, run_sharcnet(args.param_file, 
                                args.script, args.trial_name, 
                                args.run_time, args.priority,
                                verbose=args.verbose))
        else:
            print 'No Server Specified; use -coalition or -sharcnet'
    elif args.update:
        update(args.param_file, args.script, args.trial_name)
    elif args.rehash:
        if args.coalition:
            rehash(args.script, args.trial_name, 'coalition')    
        elif args.sharcnet:
            rehash(args.script, args.trial_name, 'sharcnet')
        else:
            print 'No Sever Specified; use -coalition ot -sharcnet'
    elif args.test:
        if args.coalition:
            test_coalition(args.param_file)
        elif args.sharcnet:
            test_sharcnet(args.param_file)
        else:
            print 'No Server Specififed; Use -coalition or -sharcnet'
    else:
        print 'No action; use -update, -submit, or -rehash.'
