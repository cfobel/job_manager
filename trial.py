#!/usr/bin/env python

import paramiko
from coalition.control import CoalitionControl
import yaml
from path import path
import hashlib
import getpass
import re


# TODO add a check status method for after running.

class BaseTrial(object):
    def __init__(self, ssh, sftp, out_path, exe_path, exe_name, 
                params, result_dir, time, priority, verbose=True):
        
        self.ssh = ssh
        self.verbose = verbose
        self.sftp = sftp
        self.out_path = path(out_path)
        self.exe_path = path(exe_path)
        self.id_ = None
        self.exe_name = path(exe_name)
        self.result_dir = path(result_dir)
        self.priority = priority
        self.time = time
        if isinstance(params, dict):
            params = sorted(list(params.iteritems()))
        self.params = params
        sha1 = hashlib.sha1(str((self.exe_name, self.params)))
        self.hash_path = path(sha1.hexdigest())


    def make_output_dir(self):
        # Check and see if the result directory has been made.
        if self.result_dir not in self.sftp.listdir(str(self.out_path)):
            try:
                self.sftp.mkdir(str(self.out_path/self.result_dir))
                if self.verbose: print 'created result directory'
            except:
                print 'failed to make results parent'
        elif self.verbose: 
            print 'result directory exists'
        
        result_path = self.out_path / self.result_dir
        if self.hash_path not in self.sftp.listdir(str(result_path)):
            try:
                self.sftp.mkdir(str(result_path / self.hash_path))
                if self.verbose: print 'created trial directory' 
            except:
                print 'failed to make results trial directory'
        elif self.verbose:
            print 'trial result directory exists'


    def write_config(self):
        config = (self.exe_path / self.exe_name, self.params)
        config_path = path(self.out_path / self.result_dir / self.hash_path)
        if 'config.yml' not in self.sftp.listdir(str(config_path)):
            try:
                conf_file = self.sftp.open( str(
                                             config_path / path('config.yml')),
                                             mode='w')

                conf_file.write(yaml.dump(config))
                conf_file.close()
                if self.verbose: print 'created config.yml'
            except:
                print 'failed to write config.yml'
        elif self.verbose:
            print 'config.yml already exists'


    def submit(self):
        """
        Implementation dependant: 
        Basically, call wrapper.py with 
        self.out_path/self.out_dir/self.hash_path
        as the argument and make use of time, priority,
        and id whenever possible.
        """
        dir_ = str(self.out_path/self.result_dir/self.hash_path)
        command = "%s %s" %(str(self.exe_path/path('job_manager/wrapper.py')), dir_)
        stdin, stdout, stderr = self.ssh.exec_command(command)
        output = [x for x in stdout]
        errors = [y for y in stderr]
        return output, errors


    def get_id(self):
        return self.id_


# ssh sftp and control are not kept in sync after creation
# this could cause problems. for example,
# the user may modify ssh to use a differnt user
# and then the sftp is under a different user's directory
class CoalitionTrial(BaseTrial):
    ssh = None 
    sftp = None
    control = None

    def __init__(self, exe_name, params, time, priority, result_dir, 
                 exe_path=None, out_path=None, username=None, password=""):

        if not username:
            username = getpass.getuser()
        if not exe_path:
            exe_path = '/home/coalition/'
        if not out_path:
            out_path = '/var/www/pyvpr_results/'

        if CoalitionTrial.ssh == None:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            while True:
                try:
                    ssh.connect(hostname='131.104.49.31', username=username,
                               password=password)
                    break
                except:
                    print 'Connection failed'
                    password = getpass.getpass(prompt='%s password:' %username)
                    if not password:
                        exit(1)

            CoalitionTrial.ssh = ssh

        if CoalitionTrial.sftp == None:    
            CoalitionTrial.sftp = CoalitionTrial.ssh.open_sftp()

        if CoalitionTrial.control == None:
            CoalitionTrial.control = CoalitionControl(
                                        'http://131.104.49.21:19211')

        BaseTrial.__init__(self, ssh=CoalitionTrial.ssh, 
                            sftp=CoalitionTrial.sftp,
                            out_path=out_path, exe_path=exe_path,
                            params=params, time=time, priority=priority,
                            exe_name=exe_name, result_dir=result_dir )


    def submit(self):
        dir_ = self.out_path / self.result_dir / self.hashPath
        self.id_ = CoalitionTrial.control.add(
                                affinity=str(self.result_dir.namebase), 
                                dir=self.exe_path,
                                command='wrapper %s' %dir_ )
        # use id to get status and return (output, errors) for submission 
        return list(), list()



# limitations-currently only one server can be used for each run of the script.
# The class members once set, are permanent the user would have to manually
# modify SharcNetTrial.ssh and sftp to use a different server.
class SharcNetTrial(BaseTrial):
    ssh = None
    sftp = None
    username = None

    PATH = """/opt/sharcnet/archive_tools/1.1/bin\
:/opt/sharcnet/compile/1.3/bin\
:/opt/sharcnet/vmd/1.8.7/bin\
:/opt/sharcnet/r/2.10.0/bin\
:/opt/sharcnet/namd/2.7b3/bin\
:/opt/sharcnet/ddt/2.5.1/bin\
:/opt/sharcnet/gromacs/4.0.5/bin\
:/opt/sharcnet/gaussian/g09_B.01/bin\
:/opt/sharcnet/gaussian/g09_B.01\
:/opt/sharcnet/gaussian/g03_E.01/bin\
:/opt/sharcnet/fftw/2.1.5/intel/bin\
:/opt/sharcnet/intel/11.0.083/icc/bin/intel64/\
:/opt/sharcnet/intel/11.0.083/ifc/bin/intel64/\
:/opt/sharcnet/sq-tm/2.4/bin\
:/opt/sharcnet/torque/2.5.4/bin\
:/opt/sharcnet/torque/2.5.4/sbin\
:/opt/sharcnet/torque/2.5.4/snbin\
:/opt/sharcnet/torque/2.5.4/manage\
:/opt/sharcnet/freepascal/current/bin\
:/usr/kerberos/bin:/usr/bin\
:/bin\
:/usr/sbin\
:/sbin\
:/opt/sharcnet/blast/current/bin\
:/opt/sharcnet/openmpi/1.4.2/intel/bin"""

    def __init__(self, exe_name, params, time, priority, result_dir, 
                out_path=None, exe_path=None, username=None, server='kraken'):
        if not SharcNetTrial.username:
            if not username:
                username = getpass.getuser()
            SharcNetTrial.username = username
        if not exe_path:
            exe_path = '/home/%s/' %username
        if not out_path:
            out_path = '/work/%s/pyvpr_results/' %username

        if SharcNetTrial.ssh == None:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            password = ""
            while True:
                try:
                    ssh.connect(hostname='%s.sharcnet.ca' %server, 
                                username=username, password=password)
                    break
                except:
                    print 'Connection Failed.'
                    password = getpass.getpass(prompt='%s-password' %username)
                    if not password:
                        exit(1)
            SharcNetTrial.ssh = ssh

        if SharcNetTrial.sftp == None:
            SharcNetTrial.sftp = SharcNetTrial.ssh.open_sftp()

        BaseTrial.__init__(self, ssh=SharcNetTrial.ssh, 
                                sftp=SharcNetTrial.sftp,
                                out_path=out_path, exe_path=exe_path,
                                params=params, time=time, priority=priority,
                                exe_name=exe_name, result_dir=result_dir )

    def submit(self):
        # set the PATH environment
        dir_ = self.out_path / self.result_dir / self.hash_path
        command = "PATH=%s\n sqsub -r %d -o %s python '%s %s'" % (
                   SharcNetTrial.PATH + ":/home/%s/bin" %SharcNetTrial.username,
                   self.time, str(dir_/path('log.txt')), 
                   str(self.exe_path/path('job_manager/wrapper.py')), str(dir_))

        stdin, stdout, stderr = self.ssh.exec_command(command)

        output = [line for line in stdout]
        errors = [line for line in stderr]

        for line in output:
            match = re.search(pattern='jobid\s+(?P<job>\d+)', string=line)
            if match:
                self.id_ = int(match.groupdict()['job'])
                if self.verbose: 
                    print 'self.id = ', self.id_
        
        return output, errors


"""
    sends a job to  sharcnet or coalition.
    Intended for debugging.
"""
def launch( params, state, queue, prog_path ):

    if state != 'waiting':
        return None, None

    if queue == 'local':
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname='localhost', 
                    username=getpass.getuser(), 
                    password=getpass.getpass())
        sftp = ssh.open_sftp()

        Trial = BaseTrial(ssh=ssh, sftp=sftp, 
                            exe_path='/home/rpattiso/pyvpr_example/SharCoal',
                            out_path='/home/rpattiso/pyvpr_example/SharCoal', 
                            time=1, priority=1, result_dir="new", 
                            exe_name=prog_path, params=params)

    elif queue == 'coalition': 
        Trial = CoalitionTrial(time=1, priority=1,  result_dir="100", 
                                exe_name=prog_path, params=params, 
                                username='coalition')

    elif queue == 'sharcnet':
        Trial = SharcNetTrial(time=1, priority=1,  result_dir="123", 
                               exe_name=prog_path, params=params)

    Trial.make_output_dir()
    Trial.write_config()
    return Trial.submit()


def main():    
    params = dict()
    params['word'] = 'hello world'
    params['times'] = 12
    out, err = launch(params, 'waiting', 'local', 'resound.py')
    for x in out: print x
    for y in err: print y


if __name__ == "__main__":
    main()
