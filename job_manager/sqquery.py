#!/usr/bin/env python

from trial import SharcNetConnection, SharcNetTrial
import sys
import time
from path import path
import re
from datetime import datetime, timedelta
import numpy as np
from pprint import pprint


def parse_sharcnet_jobs(jobs):
    sharcnet_pattern = r"jobid: \s+(?P<jobid>[0-9]+)\nqueue:\s+(?P<queue>\S+)\ncommand:\s+(?P<command>.+)\nsubmitted:\s+(?P<submitted>.+)\n(?P<submit_info>started:\s+(?P<started>.*)\npred_end:\s+(?P<pred_end>.*)\n)?ncpus:\s+(?P<ncpus>\d+)\nnodes:\s+(?P<nodes>.*)\nout file:\s+(?P<output>.*)\nrunlimit:\s+(?P<runtime>[0-9\.hms]+)\n"

    patt = re.compile(sharcnet_pattern)
    data = [x.groupdict() for x in patt.finditer(jobs)]
    return data

def make_sharcnet_data(data):
    time_format =  "%a %b %d %H:%M:%S %Y"

    for trial in data:
        trial['submitted'] = datetime.strptime(trial['submitted'], time_format)
        trial['jobid'] = int(trial['jobid'])
        trial['ncpus'] = int(trial['ncpus'])
        trial['output'] = path(trial['output'])
        if 'runtime' in trial and trial['runtime']:
            h_string = trial['runtime']
            hours = float(h_string[0:-1])
            assert(h_string[-1] == 'h')
            days = int(hours) / 24
            hours -= days * 24
            trial['runtime'] = timedelta(days, hours * 60 * 60, 0.0)
        if 'pred_end' in trial and trial['pred_end']:
            trial['pred_end'] = datetime.strptime(trial['pred_end'], time_format)
        if 'started' in trial and trial['started']:
            trial['started'] = datetime.strptime(trial['started'], time_format)
    return data

usage=\
"""
    <state> <lambda expression for job dictionary> <operation on list> <print expression>
    ex.
    ./sqquery.py running "job['output'].parent.parent.namebase == '92_trial_mcnc_fixed_io'"
    ie. prints all runnning  jobs with the result directory 92_trial_mcnc_fixed_io.

    States:
        running / r
        queue / q
        done / d

    job dictionary:
        key       datatype
        pred_end (datetime)
        started  (datetime)
        runtime  (deltatime)
        output   (path to log.txt)
        jobid    (int)
        ncpus    (int)
        queue    (str)
        command  (str)
        nodes    (str)
"""

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print usage
        sys.exit(0)

    judged = eval('lambda job: ' + sys.argv[2])
    operate = eval('lambda jobs: ' + sys.argv[3])
    show = eval('lambda job: ' + sys.argv[4])
    #assert(type(judged) == lambda)

    S = SharcNetConnection(username='cfobel')
    command = 'PATH=%s \nsqjobs -l%s' % (SharcNetTrial.PATH, sys.argv[1][0])
    ssh_stdin, ssh_stdout, ssh_stderr = S.exec_command(command)
    ssh_stdout.channel.recv_exit_status()
    x = parse_sharcnet_jobs(ssh_stdout.read())
    x = make_sharcnet_data(x)

    list_ = [j for j in x if judged(j)]
    list_ = operate(list_)

    if list_:
        for job in list_:
            show(job)
