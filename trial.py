#!/usr/bin/env python

import paramiko
from coalition.control import CoalitionControl
import yaml
from path import path
import hashlib
import getpass
import re
from resolve_vars import resolve_path, resolve_env_vars
from paramiko import SSHClient, SSHConfig
import sys


class Connection(object):
    def __init__(self, hostname='127.0.0.1', username=None, password=None, config_path='~/.ssh/config'):
        
        if not hostname:
            raise ValueError('Missing hostname')

        ssh_config_path = path(config_path).expand()
        if not username:
            config = SSHConfig()
            if ssh_config_path.exists():
                config.parse(ssh_config_path.open())
                if config.lookup(hostname):
                   host_config = config.lookup(hostname)
                   username = host_config['user']
                else: print 'unknown host'
            else: print 'config file path wrong'
        
        if not username:
            username = getpass.getuser()
        print username, hostname, ssh_config_path
        self.ssh = paramiko.SSHClient()
        self.ssh.load_system_host_keys()
        #self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
        while True:
            try:
                self.ssh.connect(hostname=hostname, username=username,
                               password=password)
                break
            except:
                print 'Connection failed'
                password = getpass.getpass(prompt='%s password:' %username)
                if not password:
                    sys.exit(1)
        
        self.sftp = self.ssh.open_sftp()
        self.username = username

    def mkdir(self, dir_):
        return self.sftp.mkdir(str(dir_))

    def rmdir(self, dir_):
        return self.sftp.rmdir(str(dir_))

    def listdir(self, dir_):
        return self.sftp.listdir(str(dir_))

    def open(self, file_, mode='r'):
        return self.sftp.open(str(file_), mode)

    def exec_command(self, command):
        return self.ssh.exec_command(command)

    def get_username(self):
        return self.username

    def rename(self, old, new):
        return self.sftp.rename( str(old), str(new))


class CoalitionConnection(Connection):
    def __init__(self, hostname='131.104.49.31', port=19211, username='coalition', **kwargs ):
        self.control = CoalitionControl("http://%s:%d" %(hostname, port))
        Connection.__init__(self, hostname=hostname, username=username, **kwargs )
 
    def add(self, **kwargs):
        return self.control.add(**kwargs)

class SharcNetConnection(Connection):

    def __init__(self, hostname='kraken.sharcnet.ca', **kwargs):
        Connection.__init__(self, hostname=hostname, **kwargs)        
 

# TODO add a check status method for after running.
class BaseTrial(object):
    _default_connection = None

    @property
    def connection(self):
        if self._default_connection:
            return self._default_connection
        elif self._connection:     
            return self._connection
        else:
            self._default_connection = Connection()
            return self._default_connection

    @connection.setter
    def connection(self, value):
        self._connection = value

    def _hash(self, exe_path, params):
        p = sorted( params )
        sha1 = hashlib.sha1(str((exe_path, p)))
        return path(sha1.hexdigest())

    def __init__(self, out_path, exe_path, params, time=180, 
                 priority=1, connection=None, verbose=True, env='BaseTrial.yml'):
        
        self._connection = None
        if connection:
            self.connection = connection

        self.verbose = verbose
        self.out_path = path(out_path)
        self.exe_path = path(exe_path)
        self.id_ = None
        self.priority = priority
        self.time = time
        if isinstance(params, dict):
            params = sorted(list(params.iteritems()))
        self.params = sorted(params)
        sha1 = hashlib.sha1(str((self.exe_path, self.params)))
        self.hash_path = path(sha1.hexdigest())
        env_file = path('./environments') / path(env)
        self.wrap_path = path('./')
        if env_file.isfile():
            env = yaml.load(open(env_file))
            resolve_env_vars(env)
            self.out_path = resolve_path(env, self.out_path)
            self.wrap_path = resolve_path(env, '$JOB_MANAGER_ROOT')
            self.python_path = resolve_path(env, '$WORK_PATH/local/bin/python') 
        elif verbose:
            print 'No Enviroment path found'

        print self.exe_path, self.out_path

    def make_output_dir(self):
        # Check and see if the result directory has been made.
        print 'output = ', self.out_path
        parent = self.out_path.parent
        print 'checking for ', self.out_path.namebase, ' in ',  parent
        # if user specified 'my/output/dir/' instead of 'my/output/dir'
        # this wil result in looking for nothing '' in a possibly nonexistent
        # directory 'my/output/dir'
        if not self.out_path.namebase:
            print "don't put '/' at the end of the ouput directory."
            self.out_path = parent
            parent = self.out_path.parent 
            print 'now checking for ', self.out_path, ' in ', parent

        if self.out_path.namebase not in self.connection.listdir(parent):
            print self.connection.listdir(parent)
            try:
                self.connection.mkdir(self.out_path)
                if self.verbose: print 'created result directory'
            except:
                print 'failed to make ', self.out_path
        elif self.verbose: 
            print self.out_path, ' exists'
        
        if self.hash_path not in self.connection.listdir(self.out_path):
            try:
                self.connection.mkdir(self.out_path / self.hash_path)
                if self.verbose: print 'created trial directory' 
            except:
                print 'failed to make ', self.out_path / self.hash_path
        elif self.verbose:
            print self.out_path / self.hash_path,  ' exists'

    def remove_output_dir(self):
        try:
            self.connection.rmdir(self.out_path / self.hash_path)
        except:
            print "Couldn't remove output directory"

    def write_config(self):
        config = (self.exe_path, self.params)
        config_path = path(self.out_path / self.hash_path)
        if 'config.yml' not in self.connection.listdir(config_path):
            try:
                conf_file = self.connection.open(
                                            config_path / path('config.yml'),
                                             mode='w')

                conf_file.write(yaml.dump(config))
                conf_file.close()
                if self.verbose: print 'created config.yml'
            except:
                print 'failed to write config.yml'
        elif self.verbose:
            print 'config.yml already exists'

    def remove_config(self):
        config_path = path(self.out_path / self.hash_path)
        if 'config.yml' in self.connection.listdir(config_path):
            try:
                self.connection.execute_command('rm %s' %(config_path / 'config.yml'))
            except:
                print 'filed to remove config file'

    def submit(self):
        """
        Implementation dependant: 
        Basically, call wrapper.py with 
        self.out_path/self.out_dir/self.hash_path
        as the argument and make use of time, priority,
        and id whenever possible.
        """
        dir_ = self.out_path / self.hash_path
        path_ = self.wrap_path / path('wrapper.py')
        command = "%s %s" %(path_, dir_)
        stdin, stdout, stderr = self.connection.exec_command(command)
        output = [x for x in stdout]
        errors = [y for y in stderr]
        return output, errors

    def unsubmit(self):
        # kill the process here.
        pass

    def get_state(self):
        try:
            files = self.connection.listdir(self.out_path / self.hash_path)
        except:
            print  self.out_path / self.hash_path, ' not found'
            return None
        if '.finished' in files:
            return 'finished'
        elif '.error' in files:
            return 'error'
        else:
            print 'in progress? ', files
            return None

    def get_id(self):
        return self.id_


# ssh sftp and control are not kept in sync after creation
# this could cause problems. for example,
# the user may modify ssh to use a differnt user
# and then the sftp is under a different user's directory
class CoalitionTrial(BaseTrial):
    _default_connection = None #CoalitionConnection()

    def __init__(self, params, time=180, priority=1, env='coalition.yml',
                 exe_path=None, out_path=None, connection=None ):

        BaseTrial.__init__(self, connection=connection,
                            out_path=out_path, exe_path=exe_path,
                            params=params, time=time, priority=priority, env=env)
    @property
    def connection(self):
        if self._default_connection:
            return self._default_connection
        elif self._connection:     
            return self._connection
        else:
            self._default_connection = CoalitionConnection()
            return self._default_connection


    def submit(self):
        dir_ = self.out_path / self.hash_path
        self.id_ = self.connection.add(
                         affinity=str(self.out_path.namebase), 
                         dir=self.exe_path.parent,
                         command='%s %s' %( self.wrap_path / path('wrapper.py'), dir_) )
        # use id to get status and return (output, errors) for submission 
        return list(), list()

    def unsubmit(self):
        print 'command unsubmit not supported yet.'



# limitations-currently only one server can be used for each run of the script.
# The class members once set, are permanent the user would have to manually
# modify SharcNetTrial.ssh and sftp to use a different server.
class SharcNetTrial(BaseTrial):
    _default_connection = None #SharcNetConnection()
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

    def __init__(self, params, time=180, priority=1, 
                out_path=None, exe_path=None, connection=None, env='sharcnet.yml'):
       
        BaseTrial.__init__(self, connection=connection, 
                                out_path=out_path, exe_path=exe_path,
                                params=params, time=time, priority=priority, env=env)

    @property
    def connection(self):
        if self._default_connection:
            return self._default_connection
        elif self._connection:     
            return self._connection
        else:
            self._default_connection = SharcNetConnection()
            return self._default_connection


    def submit(self):
        # set the PATH environment
        dir_ = self.out_path / self.hash_path
        command = "PATH=%s\n sqsub -r %d -o %s %s %s %s" % (
                   SharcNetTrial.PATH + ":/home/%s/bin" %self.connection.get_username(),
                   self.time, str(dir_/path('log.txt')), 
                   self.python_path,
                   str(self.wrap_path / path('wrapper.py')), str(dir_))
        
        print command
        stdin, stdout, stderr = self.connection.exec_command(command)

        output = [line for line in stdout]
        errors = [line for line in stderr]

        for line in output:
            match = re.search(pattern='jobid\s+(?P<job>\d+)', string=line)
            if match:
                self.id_ = int(match.groupdict()['job'])
                if self.verbose: 
                    print 'self.id = ', self.id_
        
        return output, errors

    def unsubmit(self):
        self.connection.execute_command('sqkill %d' %self.id_)


"""
    sends a job to  sharcnet or coalition.
    Intended for debugging.
"""
def launch( params, state, queue, prog_path, out_path ):

    if state != 'waiting':
        return None, None

    if queue == 'local':
        BaseTrial._default_connection = Connection()
        Trial = BaseTrial(  exe_path=prog_path,
                            out_path=out_path, 
                            time=1, priority=1, params=params)

    elif queue == 'coalition':
        CoalitionTrial._default_connection = CoalitionConnection() 
        Trial = CoalitionTrial(time=1, priority=1, params=params, 
                               exe_path=prog_path,
                               out_path=out_path )

    elif queue == 'sharcnet':
        SharcNetTrial._default_connection = SharcNetConnection()
        Trial = SharcNetTrial(time=1, priority=1, params=params, 
                               exe_path=prog_path,
                               out_path=out_path )

    Trial.make_output_dir()
    Trial.write_config()
    return Trial.submit()
