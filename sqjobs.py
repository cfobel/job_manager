#!/usr/bin/env python

from trial import SharcNetConnection, SharcNetTrial
import sys
import time
from path import path
import re
from datetime import datetime, timedelta 
import numpy as np

def parse_sharcnet_jobs(jobs):
    sharcnet_pattern = r"jobid: \s+(?P<jobid>[0-9]+)\nqueue:\s+(?P<queue>\S+)\ncommand:\s+(?P<command>.+)\nsubmitted:\s+(?P<submitted>.+)\n(?P<submit_info>started:\s+(?P<started>.*)\npred_end:\s+(?P<pred_end>.*)\n)?ncpus:\s+(?P<ncpus>\d+)\nnodes:\s+(?P<nodes>.*)\nout file:\s+(?P<output>.*)\nrunlimit:\s+(?P<runtime>[0-9\.hms]+)\n"
    time_format =  "%a %b %d %H:%M:%S %Y"

    patt = re.compile(sharcnet_pattern)
    data = [x.groupdict() for x in patt.finditer(jobs)]

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
    {r, q, d, z}
    r = running
    q = queued
    d = done
    z = suspended
"""

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print usage
        sys.exit(0)

    S = SharcNetConnection(username='cfobel')
    #make sure temp.txt does not exist here
    command = 'PATH=%s \nsqjobs -l%s > /home/%s/temp.txt' % (SharcNetTrial.PATH, sys.argv[1][0], S.get_username())
    S.exec_command(command)
    time.sleep(15)
    f = S.open('/home/%s/temp.txt' % S.get_username())
    x = parse_sharcnet_jobs(f.read())
    f.close()

    job_names = set([y['output'].parent.parent.namebase for y in x])
    job_indices = dict()
    job_times = dict()
    job_sub_times = dict()

    for job in job_names:
        job_indices[job] = list()
        job_times[job] = list()
        job_sub_times[job] = list()

    for i in range(len(x)):
        key = x[i]['output'].parent.parent.namebase
        job_indices[key].append(i)
        if 'started' in x[i]:
            job_times[key].append(x[i]['runtime'])
        job_sub_times[key].append(x[i]['submitted'])

    now = datetime.now()

    print ''
    if sys.argv[1][0] == 'r': 
        print '    #     min_runtime       max_runtime      job_result_dir  '
        print '--------------------------------------------------------------'
        for job in job_names:
            size = len(job_times[job])
            diffs = job_times[job] #[now - then for then in job_times[job]]
            min_ = min(diffs)
            max_ = max(diffs)
            readable = lambda time: '  %2dd  %2d:%02d:%02d  ' % (time.days, time.seconds/3600, time.seconds%3600/60, time.seconds % 60)
            print '%5d' % len(diffs), readable(min_), readable(max_), job
    elif sys.argv[1][0] == 'q':
        print '    #          min_submit                  max_submit                  job_result_dir  '
        print '------------------------------------------------------------------------------------------'
        for job in job_names:
            size = len(job_sub_times[job])
            diffs = job_sub_times[job]
            min_ = min(diffs)
            max_ = max(diffs)
            print '%5d' % len(diffs), '   ', min_.ctime(), '   ', max_.ctime(), '  ', job
    elif sys.argv[1][0] == 'd':
        print '    #     min_runtime       max_runtime      job_result_dir  '
        print '--------------------------------------------------------------'
        for job in job_names:
            size = len(job_times[job])
            diffs = job_times[job] 
            min_ = min(diffs)
            max_ = max(diffs)
            readable = lambda time: '  %2dd  %2d:%02d:%02d  ' % (time.days, time.seconds/3600, time.seconds%3600/60, time.seconds % 60)
            print '%5d' % len(diffs), readable(min_), readable(max_), job
    print '_____________________________________________________________'


