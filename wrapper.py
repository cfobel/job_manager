#!/usr/bin/env python

from datetime import datetime
import sys
from path import path
import yaml
import subprocess
import os
from resolve_vars import resolve_env_vars


help_string = \
"""
    This script is intended to open a tuple object from
    a config.yml file in the form
    (exec,params)
    where params = [(k, v)] an ordered list of tuples.
    the list must be ordered so that it can be recreated and hashed
    to give th result directory where that parameter list was executed.

    The first argument is the path at which this script will execute.
    It will immediately look for an config.yml file.
    
    usage:  wrapper <path to  parent of config.yml>
"""


def run(result_path, parent, verbose=True):
    path_ = path(result_path)

    if not path_.exists:
        print 'Error: Path does not exist' 
        exit(1)
    
    if path_.isdir:
        config_path = path_ / 'config.yml'
        if not config_path.exists:
            # any yaml file? or error
            pass

    if path(parent / path('environments/env_vars.yml')).isfile():
        paths = yaml.load(open(parent / 'environments/env_vars.yml', 'r'))
        resolve_env_vars(paths)
        for k , v in paths.iteritems():
            os.environ[k] = ':'.join(v)
    elif verbose:
        print 'no path variable file found.'
                
    try:
        yam = open(config_path)
    except:
        log = open(path_ / '.error', 'w')
        log.write('yaml open')
        log.close()
        print "couldn't open config path ", config_path
        exit(1)

    pkg = yaml.load(yam)
    
    # TODO allow for list arguments as well not in tuple form?.
    params = [str(pkg[0])] # add executable name ./program.py
    
    for k, v in pkg[1]:
        if k and v is not None:
            params.append('-%s' %k)
            params.append('%s' %v)
        elif v is not None:
            params.append('%s' %v)
        elif k:
            params.append('-%s' %k)
    # all programs must accept '-o output dir'
    params.append('-o')
    params.append('%s' %result_path)
    
    start_time = datetime.now()

    try:
        #subprocess.call(params, shell=True)
        command = ' '.join(params)
        print command
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        print p.communicate()
    except:
        log = open(path_ / '.error', 'w' )
        log.write('execute')
        log.close()
        exit(1)

    end_time = datetime.now()
    log = open(path_ / '.finished', 'w')
    log.write("start time: %s\nend time: %s" %(start_time, end_time))
    log.close()


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print help_string
        exit(1)
    else:
        parent = path(sys.argv[0]).parent        
        run(sys.argv[1], parent)

