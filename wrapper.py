#!/usr/bin/env python

from datetime import datetime
import sys
from path import path
import yaml
import subprocess
import os
from resolve_vars import resolve_env_vars
import platform
import re

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

def set_environment(verbose=False):
    hostname = platform.node()
    env_file = path(__file__).parent.joinpath('environments').joinpath('env_vars.yml')
    if not env.exists():
        if verbose:
            print 'env_vars file not found.'
        return
    else:    
        envs = yaml.load(env_file.open())
        for k, v in env.iteritems():
            os.eniron[k] = v

def run(result_path, parent, verbose=False):
    msg = ''

    path_ = path(result_path)
    if not path_.exists(): 
        return (1, 'Error: Result Path does not exist.')
    
    if path_.isdir:
        config_path = path_ / 'config.yml'
        if not config_path.exists():
            return (1, '%s does not exists'%config_path)

    if path(parent / path('environments/env_vars.yml')).isfile():
        paths = yaml.load(open(parent / 'environments/env_vars.yml', 'r'))
        resolve_env_vars(paths)
        for k, v in paths.iteritems():
            os.environ[k] = ':'.join(v)
    elif verbose:
        print 'no path variable file found.'

    try:
        yam = open(config_path)
    except Exception, e:
        print "couldn't open config path ", config_path
        return (1, 'yaml open config: %s' %e.args)

    pkg = yaml.load(yam)
    # TODO allow for list arguments as well not in tuple form?.
    params = [str(pkg[0])] # add executable name ./program.py
    if verbose:
        print 'Wrapper Executable and paramters', pkg

    for k, v in dict(pkg[1]).iteritems():
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

    try:
        #subprocess.call(params, shell=True)
        command = ' '.join(params)
        if verbose:
            print 'In Wrapper Command = ', command
        p = subprocess.Popen(command, stdout=subprocess.PIPE, 
                                stderr=subprocess.PIPE, shell=True)
        p.communicate()
        ret = p.wait()
    except Exception, e:
        ret = 1
        msg = str(e.args)

    return (ret, msg)


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print help_string
        exit(1)
    if len(sys.argv) == 3 and sys.argv[2] == '-verbose':
        verbose = True
    else:
        verbose = False

    set_environment(verbose)        
    start_time = datetime.now()
    parent = path(sys.argv[0]).parent
    path_ = path(sys.argv[1])
    ret = run(sys.argv[1], parent, verbose=verbose)

    if ret[0] != 0:        
        log = open(path_ / '.error', 'w' )
        log.write('Error Code: %d\n'%ret[0])
    else:
        end_time = datetime.now()
        log = open(path_ / '.finished', 'w' )
        log.write("start time: %s\nend time: %s" %(start_time, end_time))

    log.write(ret[1])
    log.close()  

