#!/usr/bin/env python

from trial import CoalitionTrial, SharcNetTrial, BaseTrial, Trial, SharcNetConnection, CoalitionConnection, Connection
import shelve
from path import path
import yaml
import time
import sys

def count(trial_file):
    return len([k for k, v in shelve.open(trial_file).items() if k not in ['default_script', 'default_results']])


def wait(trial_file, exe_path, out_path, interval):
    print 'Checking for updates every %d minutes' %interval
    while _update(trial_file, exe_path, out_path)[0]:
        print '.',        
        time.sleep(interval * 60)
    print 'All Done.'


# Should save id as well so it can be killed later if needed
def submit_all(trial_file, trial_list):
    trial = shelve.open(args.param_file, writeback=True)
 
    length = len(trial_list)
    tenth = max((length / 10), 1)
    if length == 0:
        print 'No trials.'
        return
    print 'Creating configuration files and directories, this may take awhile.'
    for i, (T, p) in enumerate(trial_list):
        T.make_output_dir()
        T.write_config()
        if i % tenth == 0:
            print '%d%%..'%((100. * i) / length),
            sys.stdout.flush()
    print ''

    print 'Submitting jobs...'
    for i, (T, p) in enumerate(trial_list):
        T.submit()
        trial[p][Trial.ID] = T.get_id()
        trial[p][Trial.STATE] = Trial.SUBMITTED
        trial[p][Trial.QUEUE] = T.get_server()
        if i % tenth == 0:
            print '%d%%..'%((100. * i) / length),
            sys.stdout.flush() 
    print ''

    print '%d jobs submitted!' %length
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
    parser.add_argument('-test', action='store_true')
    parser.add_argument('-fake_submit', action='store_true')
    parser.add_argument('-reset_errors', action='store_true')
    parser.add_argument('-wait', nargs=1, type=int)
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
    parser.add_argument('-count', action='store_true')

    args = parser.parse_args()
    if args.wait:
        args.wait = args.wait[0]

    if args.submit:
        args.run_time = args.run_time[0]
        if args.priority:   
            args.priority = args.priority[0]
        else:
            args.priority = 1    
    args.param_file = args.param_file[0]

    trial = shelve.open(args.param_file)
    if args.trial_name:
        args.trial_name = args.trial_name[0]
    if args.script:
        args.script = args.script[0]
        #check for 'python' and .py
    else:
        if 'default_script' in trial:
            args.script = path(trial['default_script'])
            if 'default_results' in trial and not args.trial_name:
                args.trial_name = trial['default_results']
                if args.verbose:
                    print 'Assumming Default Results "%s"'%args.trial_name
            if args.verbose:
                print 'Assumming Default Script "%s"'%args.script
        else:
            print 'No script specified and no default in param_file.'
            trial.close()
            sys.exit(1)
    
    if not args.trial_name:
        args.trial_name = '${PYVPR_RESULTS}/' + path(args.param_file).namebase
        if args.verbose:        
            print 'Results directory autoset to "%s"'%args.trial_name

    trial['default_results'] = args.trial_name
    trial['default_script'] = args.script
    trial.close()
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

        # This is a fix for unsorted parameters.
        data = yaml.load(cfile)
        sort_params = sorted(data[1])
        new_path = T.out_path / T._hash(data[0], sort_params)
        cfile.close()

        if new_path == T.out_path / folder:
            print folder, ' already in right spot'
            continue
        
        # rewrite the sorted parameters into the config file
        cfile = T.connection.open(T.out_path / config, 'w')
        cfile.write(yaml.dump(data))
        cfile.close()
        T.connection.rename(T.out_path / folder, new_path)


def _update(param_file, exe_path, out_path):
    entry = shelve.open(param_file, writeback=True)
    finished = 0
    parameters = entry.iteritems()
    num_trials = 0
    errors = 0
    running = 0
    waiting = 0
    
    for k, v in parameters:
        if k == 'default_script' or k == 'default_results':
            continue
        num_trials += 1    
        if v[Trial.STATE] != Trial.SUBMITTED:
            if v[Trial.STATE] == Trial.FINISHED:
                finished += 1            
            elif v[Trial.STATE] == Trial.ERROR:
                errors += 1
            elif v[Trial.STATE] == Trial.WAITING:
                waiting += 1           
            else:
                print v[Trial.STATE]
            continue

        if v[Trial.QUEUE] == Trial.SHARCNET:
            if SharcNetTrial._default_connection == None:
                C = SharcNetConnection(username='cfobel')
                SharcNetTrial._default_connection = C
            T = SharcNetTrial(params=eval(k), exe_path=exe_path, 
                                            out_path=out_path)
        elif v[Trial.QUEUE] == Trial.COALITION:
            if CoalitionTrial._default_connection == None:
                C = CoalitionConnection()
                CoalitionTrial._default_connection = C
            T = CoalitionTrial(params=eval(k), exe_path=exe_path, 
                                                out_path=out_path)
        elif v[Trial.QUEUE] == Trial.LOCAL:
            if BaseTrial._default_connection == None:
                C = Connection()
                BaseTrial._default_connection = C
            T = BaseTrial(params=eval(k), exe_path=exe_path, 
                                          out_path=out_path)
        else:
            print 'Unknown Queue ', v[Trial.QUEUE] 
            continue

        new_state = T.get_state()
        if not new_state:
            running += 1
            continue
        else:
            v[Trial.STATE] = new_state
            if new_state == Trial.FINISHED:
                finished += 1
            elif new_state == Trial.ERROR:
                errors += 1
            entry[k] = v
    entry.close()
    return (num_trials - finished, finished, num_trials, errors, running, waiting)


def update(param_file, exe_path, out_path):
    ret = _update(param_file, exe_path, out_path)
    left, finished, num_trials, errors, running, waiting = ret
    if not left:
        print 'All %d are done' % finished
    elif finished:
        print '%d of %d have finished' % (finished, num_trials)
    if errors:
        print '%d have errors' % errors
    if running:
        print '%d are running.' % running
    if waiting:
        print '%d are waiting.' % waiting

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

def set_default_script(trial_file, script):
    trial_file['default_script'] = script

def set_default_result_dir(trial_file, dir_):
    trial_file['default_results'] = dir_


def run_coalition(trial_file, prog_path, result_path, run_time, priority,
                                                             verbose=False):
    return run_parameters(trial_file, lambda x: False, lambda x: True, lambda x: False, 
                prog_path, result_path, run_time, priority, verbose=verbose)


def run_sharcnet(trial_file, prog_path, result_path, run_time, priority, 
                                                            verbose=False):
    return run_parameters(trial_file, lambda x: True, lambda x: False, lambda x: False, 
            prog_path, result_path, run_time, priority, verbose=verbose)

def run_local(trial_file, prog_path, result_path, run_time, priority, 
                                                            verbose=False):
    return run_parameters(trial_file, lambda x: False, lambda x: False, lambda x: True, 
            prog_path, result_path, run_time, priority, verbose=verbose)


def run_parameters(trial_file, sharc_filter=lambda x: False,
                                coalition_filter=lambda x: False,
                                local_filter=lambda x: False,
                                 prog_path='', result_path='', run_time=10080,
                                priority=1, verbose=False, test=False):

    trial =  shelve.open(trial_file, writeback=True)
    trial_objs = list()
    parameters = trial.keys()

    already_submitted = 0
    for p in parameters:
        if p == 'default_script' or p == 'default_results':
            continue
        if trial[p][Trial.STATE] == Trial.WAITING:
            assert(trial[p][Trial.QUEUE] == None)
            assert(trial[p][Trial.ID] == None)
            if sharc_filter(p):
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
                T = CoalitionTrial(out_path=result_path, 
                                    exe_path=prog_path, 
                                    params=eval(p), 
                                    time=run_time, 
                                    priority=priority,
                                    verbose=verbose,
                                    test=test)
            elif local_filter(p):
                T = BaseTrial(out_path=result_path, 
                                    exe_path=prog_path, 
                                    params=eval(p), 
                                    time=run_time, 
                                    priority=priority,
                                    verbose=verbose,
                                    test=test)
            else:
                continue
            trial_objs.append((T, p))
        else:
            already_submitted += 1
    if already_submitted > 0:
        print '%d Were already submitted; And Trials were not created for them' %already_submitted
    print '%d Trials were created' %len(trial_objs)
    trial['default_script'] = prog_path
    trial['default_results'] = result_path
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

def set_as_submitted(param_file, queue):
	entry = shelve.open(param_file)
	for k, v in entry.iteritems():
		if k == 'default_results' or k == 'default_script':
			continue
		v[Trial.QUEUE] = queue
		v[Trial.STATE] = Trial.SUBMITTED
	entry.close()


def reset_errors(param_file):
	entry = shelve.open(param_file, writeback=True)
	for k, v in entry.iteritems():
		if k == 'default_results' or k == 'default_script':
			continue
        if v[Trial.STATE] == Trial.ERROR:
            entry[k][Trial.QUEUE] = None
            entry[k][Trial.STATE] = Trial.WAITING
            entry[k][Trial.ID] = None
            print 'you must delete the result folders that errored.'
	entry.close()


def show(param_file, state_var, value):
	entry = shelve.open(param_file)
	script = entry['default_script']
	results = entry['default_results']
	for k, v in entry.iteritems():
		if k == 'default_results' or k == 'default_script':
			continue
		if value == None:
			print v[str(state_var)],
			print 'Parameters:', dict(eval(k))
			print 'Results directory:', path(results)/BaseTrial._hash(script, eval(k))
			print ''
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
        elif args.local: 
            submit_all(args.param_file, run_local(args.param_file, 
                                args.script, args.trial_name, 
                                args.run_time, args.priority,
                                verbose=args.verbose))
        else:
            print 'No Server Specified; use -coalition or -sharcnet'

    elif args.update:
        if args.wait:
            wait(args.param_file, args.script, args.trial_name, args.wait)
        else:        
            update(args.param_file, args.script, args.trial_name)

    elif args.rehash:
        if args.coalition:
            rehash(args.script, args.trial_name, 'coalition')    
        elif args.sharcnet:
            rehash(args.script, args.trial_name, 'sharcnet')
        else:
            print 'No Sever Specified; use -coalition or -sharcnet'
    elif args.fake_submit:
		if args.coalition:
			queue = Trial.COALITION
		elif args.sharcnet:
			queue = Trial.SHARCNET
		elif args.localhost:
			queue = Trial.LOCAL
		else:
			print 'No Server Specified; use -coalition or -sharcnet'
		set_as_submitted(args.param_file, queue)
    elif args.reset_errors:
			reset_errors(args.param_file)
    elif args.count:
        print count(args.param_file)
    elif args.test:
        if args.coalition:
            test_coalition(args.param_file)
        elif args.sharcnet:
            test_sharcnet(args.param_file)
        else:
            print 'No Server Specififed; Use -coalition or -sharcnet'
    else:
        print 'No action; use -update, -submit, or -rehash.'
