#! /usr/bin/env python


import sys
from path import path
import shelve

from manager import trial_from_config
from trial import Trial
from sharcnet_trial import SharcNetTrial
from sqjobs import get_sharcnet_jobs


usage=\
"""
    <param_file>
    pass in the parameter file to be updated by the state of sharcnet.
"""

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print usage
        sys.exit(1)

    # get the results directory from the database
    db_path = path(sys.argv[1])
    db = shelve.open(db_path, writeback=True)
    result_dir = path(db['default_results'])
    script = path(db['default_script'])
    params = db.keys()[0]
    db.close()

    # use some data from the database to get the resolved output_dir and connection.
    sharc_trial = SharcNetTrial(out_path=result_dir, exe_path=script, params=params)
    connection = sharc_trial.connection

    # get_all the hashes for this group of jobs.
    hashes = set(connection.listdir(sharc_trial.out_path))
    print 'Trials in db', len(hashes)

    # collect the submitted hash_paths from the sharcnet queues
    submitted = set()
    queue = get_sharcnet_jobs(connection, 'q')
    for q in queue:
        hash_q = q['output'].parent.namebase
        if hash_q in hashes:
            submitted.add(hash_q)
    subq = len(submitted)
    print 'Submitted Queue', subq

    running = get_sharcnet_jobs(connection, 'r')
    for r in running:
        hash_r = r['output'].parent.namebase
        if hash_r in hashes:
            submitted.add(hash_r)
    print 'Running Queue', len(submitted) - subq

    # Check for .finished and .error files in the results dirs:
    remaining = hashes.difference(submitted)
    finished = set()
    errored = set()
    for res_dir in remaining:
        files = connection.listdir(sharc_trial.out_path/res_dir)
        if '.finished' in files:
            finished.add(res_dir)
        elif '.error' in files:
            errored.add(res_dir)

    print 'Errors', len(errored)
    print 'Finished', len(finished)

    # Create trial_objects for the submitted hashes to get parameters.
    submitted_trials = []
    missing_config = set()
    for result_dir in submitted.union(finished).union(errored):
        config_dir = path(sharc_trial.out_path/result_dir)
        if 'config.yml' in connection.listdir(config_dir):
            f = connection.open(config_dir/'config.yml')
            submitted_trials.append(trial_from_config(config_dir/'config.yml', f, 'sharcnet'))
            f.close()
        else:
            missing_config.add(sharc_trial.out_path)

    print 'Missing Config', len(missing_config)
    print 'Trials from db that were submitted', len(submitted_trials)
    # update the database to submitted

    # reset the submissions in the database
    db = shelve.open(db_path, writeback=True)
    for k, v in db.items():
        if k in ['default_results', 'default_script']:
            continue
        v['state'] = Trial.WAITING
        v['queue'] = None
        db[k] = v

    for trial in submitted_trials:
        hash_path = trial.out_path.parent.namebase
        if hash_path in finished:
            db[str(tuple(trial.params))]['state'] = Trial.FINISHED
        elif hash_path in errored:
            db[str(tuple(trial.params))]['state'] = Trial.ERRORED
        else:
            db[str(tuple(trial.params))]['state'] = Trial.SUBMITTED
        db[str(tuple(trial.params))]['queue'] = Trial.SHARCNET
    db.close()
