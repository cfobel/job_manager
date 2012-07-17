#!/usr/bin/env python
import traceback

import paramiko
from paramiko import SSHClient, SSHConfig
from coalition.control import CoalitionControl
import yaml
from path import path
import hashlib
import getpass
import re
from resolve_vars import resolve, resolve_env_vars
import sys
import os
from port_forward import forward_tunnel



#TODO
"""
    Add __repr__ or state method so that a trial
    can be written to a file and brought back later to check on it's status.
    Bring the functionality from the filter script.
"""




def retry_on_fail(f, retry_count=5):
    def new_f(self, *args, **kwargs):
        for i in range(retry_count):
            try:
                retval = f(self, *args, **kwargs)
                break
            except Exception as e:
                if not isinstance(e, IOError):
                    print '[retry_on_fail] %s %s %s' % (type(e), e, e.args)
                    traceback.print_exc()
                    self.connect()
                    if i >= retry_count - 1:
                        raise
                    retval = None
        return retval
    return new_f


# Constants
class Trial:
    # Servers
    COALITION = 'coalition'
    SHARCNET = 'sharcnet'
    LOCAL = 'local'

    # Job Keys
    QUEUE = 'queue'
    STATE = 'state'
    ID = 'id'

    # Job States
    SUBMITTED = 'submitted'
    WAITING = 'waiting'
    ERROR = 'error'
    FINISHED = 'finished'

    # Files
    CONFIG = 'config.yml'
    WRAPPER = 'wrapper.py'
    LOG = 'log.txt'

    # Variable Paths
    EXPERIMENTS = path('${PYVPR_EXPERIMENTS}')
    RESULTS  = path('${PYVPR_RESULTS}')
    MANAGER = path('${JOB_MANAGER_ROOT}')
    WORK = path('${WORK_PATH}')
    BENCHMARKS = path('${BENCHMARK_PATH}')


class Connection(object):
    def __init__(self, hostname='localhost', username=None, password=None,
                config_path='~/.ssh/config', port=None, verbose=False):
        self.verbose = True
        if not hostname:
            raise ValueError('Missing hostname')
        self.sftp = None
        self.ssh = None
        ssh_config_path = path(config_path).expand()
        if not username:
            print 'no username'
            config = SSHConfig()
            if ssh_config_path.exists():
                config.parse(ssh_config_path.open())
                if config.lookup(hostname):
                   host_config = config.lookup(hostname)
                   username = host_config['user']
                else:
                    if self.verbose:
                        print 'Unknown host ', hostname
            else: print 'config file path wrong'

        self.verbose = verbose
        if not username:
            username = getpass.getuser()
        if self.verbose:
            print 'Connection info: ', username, hostname, ssh_config_path

       #self.ssh.set_missing_host_key_policy(
        #    paramiko.AutoAddPolicy())
        self.hostname = hostname
        self.username = username
        self.password = password
        self.connect()


    def connect(self):
        del self.sftp
        del self.ssh
        self.ssh = paramiko.SSHClient()
        self.ssh.load_system_host_keys()
        while True:
            try:
                self.ssh.connect(hostname=self.hostname,
                                username=self.username,
                                password=self.password)
                break
            except Exception as e:
                print 'Connection failed: (%s)' % e, e.args
                self.password = getpass.getpass(prompt='%s@%s password:' %(self.username, self.hostname))
                if not self.password:
                    sys.exit(1)
        self.sftp = self.ssh.open_sftp()

    @retry_on_fail
    def remove(self, path_):
        if self.verbose:
            print 'remove(', path_, ')'
        return self.sftp.remove(str(path_))

    @retry_on_fail
    def mkdir(self, dir_):
        if self.verbose:
            print 'mkdir(', dir_, ')'
        return self.sftp.mkdir(str(dir_))

    @retry_on_fail
    def rmdir(self, dir_):
        if self.verbose:
            print 'rmdir(', dir_, ')'
        return self.sftp.rmdir(str(dir_))


    @retry_on_fail
    def listdir(self, dir_):
        if self.verbose:
            print 'listdir( ', dir_, ')'
        return self.sftp.listdir(str(dir_))


    @retry_on_fail
    def open(self, file_, mode='r'):
        if self.verbose:
            print 'open( ', file_, ', ', mode, ')'
        return self.sftp.open(str(file_), mode)


    @retry_on_fail
    def exec_command(self, command):
        if self.verbose:
            print 'exec_command( ', command, ')'
        return self.ssh.exec_command(command)


    @retry_on_fail
    def get_username(self):
        return self.username


    @retry_on_fail
    def rename(self, old, new):
        if self.verbose:
            print 'rename( ', old, ', ', new, ')'
        return self.sftp.rename( str(old), str(new))


class DirectConnection(Connection):
    def __init__(self):
        pass

    def rename(self, old, new):
        os.rename(path(old).expand(), path(new).expand())

    def get_username(self):
        return os.getenv('LOGNAME')

    def _open(self, file_, mode='r'):
        return open(path(file_).expand(), mode)

    def exec_command(self, command):
        f = os.popen(command)
        output = [o for o in f]
        f.close()
        return output

    def listdir(self, dir_):
        f = os.popen('ls -a %s' % path(dir_).expand())
        list_ = [l.strip() for l in f]
        f.close()
        return list_

    def mkdir(self, dir_):
        os.mkdir(path(dir_).expand())

    def rmdir(self, dir_):
        os.rmdir(path(dir_).expand())

DirectConnection.open = DirectConnection._open


class CoalitionConnection(Connection):
    #os.system('ssh -Nf -L%d:localhost:%d %s@%s'%(19211, 19211, 'coalition', 'tobias.socs.uoguelph.ca'))

    @property
    def control(self):
        if not self._control:
            self._control = CoalitionControl("http://localhost:%d" %self.port)
            #forward_tunnel(self.port,'localhost', self.port, self.ssh.get_transport())
        return self._control

    def __init__(self, hostname='tobias.socs.uoguelph.ca', port=19211,
                 username='coalition', **kwargs ):
        Connection.__init__(self, hostname=hostname,
                            username=username, **kwargs)
        self.port = port
        self._control = None

    def run(self, **kwargs):
        return self.control.add(**kwargs)


class SharcNetConnection(Connection):
    def __init__(self, hostname='orca.sharcnet.ca', username='cfobel', **kwargs):
        Connection.__init__(self, hostname=hostname, username=username, **kwargs)


class BaseTrial(object):
    _default_connection = None
    share_connections = True #False

    def get_server(self):
        return Trial.LOCAL

    def _get_default_connection(self):
        return DirectConnection()

    @property
    def connection(self):
        if self.share_connections:
            if self._default_connection:
                return self._default_connection
            elif self._connection:
                return self._connection
            else:
                self._default_connection = self._get_default_connection()
                return self._default_connection
        else:
            if self._connection:
               return self._connection
            else:
               self._connection = self._get_default_connection()
               return self._connection

    @connection.setter
    def connection(self, value):
        self._connection = value

    @classmethod
    def _hash(cls, exe_path, params):
        p = sorted( params )
        sha1 = hashlib.sha1(str((exe_path, p)))
        return path(sha1.hexdigest())


    def __init__(self, out_path, exe_path, params, time=10080,
                priority=1, connection=None, verbose=False,
                test=False, env='BaseTrial.yml', block=True):
        self.block = block
        self._connection = None
        if connection:
            self.connection = connection
        self.test = test
        self.verbose = verbose
        self.out_path = path(out_path)
        self.exe_path = path(exe_path)
        self.id_ = None
        self.priority = priority
        self.time = time
        root = path(__file__).parent
        if isinstance(params, dict):
            params = sorted(list(params.iteritems()))
        self.params = sorted(params)
        if verbose:
            print self.params
        sha1 = hashlib.sha1(str((self.exe_path, self.params)))
        self.hash_path = path(sha1.hexdigest())
        env_file = root / 'environments' / env
        self.wrap_path = path('./')

        if env_file.isfile():
            env = yaml.load(open(env_file))
            self.env = env
            resolve_env_vars(env)
            if self.verbose:
                print 'outpath before sub ', self.out_path
            self.out_path = resolve(env, self.out_path)
            self.wrap_path = resolve(env, Trial.MANAGER)
            self.python_path = resolve(env, Trial.WORK) / 'local/bin/python'
        elif verbose:
            print 'No Enviroment path found'
        if self.verbose:
            print  'exe path, out path, ', self.exe_path, self.out_path


    def make_output_dir(self):
        # Check and see if the result directory has been made.
        if self.verbose:
            print 'full output path = ', self.out_path
        parent = self.out_path.parent
        #if self.verbose:
        #    print 'checking for ', self.out_path.namebase, ' in ',  parent
        # if user specified 'my/output/dir/' instead of 'my/output/dir'
        # this wil result in looking for nothing '' in a possibly nonexistent
        # directory 'my/output/dir'
        if not self.out_path.namebase:
            print "don't put '/' at the end of the ouput directory."
            self.out_path = parent
            parent = self.out_path.parent
            #print 'now checking for ', self.out_path, ' in ', parent

        #if self.out_path.namebase not in self.connection.listdir(parent):
        #    if self.verbose:
        #        print '%s not int %s!!!' %(self.out_path.namebase, parent)
        #        print 'parent to result dir = ', self.connection.listdir(parent)
        try:
            ssh_streams =  self.connection.mkdir(self.out_path)
            if self.block and ssh_streams:
                ssh_streams[1].channel.recv_exit_status()
            if self.verbose:
                print 'created result directory'
        except Exception as e:
            if self.verbose:
                print 'failed to make ', self.out_path, e.args
                #sys.exit(1)
        #elif self.verbose:
        #    print self.out_path, ' exists'

        #if self.hash_path not in self.connection.listdir(self.out_path):
        try:
            ssh_stdin, ssh_stdout, ssh_stderr = self.connection.mkdir(self.out_path / self.hash_path)
            if self.block:
                exit = ssh_stdout.channel.recv_exit_status()
            if exit:
                raise Exception('Failed to make trial directory')
            if self.verbose: print 'created trial directory'
        except:
            if self.verbose:
                print 'failed to make ', self.out_path / self.hash_path
        #elif self.verbose:
        #    print self.out_path / self.hash_path,  ' exists'


    def remove_output_dir(self):
        try:
            files = self.connection.listdir(self.out_path/self.hash_path)
            for f in files:
                self.connection.remove(f)
            self.connection.rmdir(self.out_path / self.hash_path)
        except:
            print "Couldn't remove output directory"


    def write_config(self):
        config = (self.exe_path, self.params)
        if self.verbose:
            print 'Config = ', config
        config_path = path(self.out_path / self.hash_path)
        #if Trial.CONFIG not in self.connection.listdir(config_path):
        try:
            conf_file = self.connection.open(
                                        config_path / path(Trial.CONFIG),
                                         mode='w')
            conf_file.write(yaml.dump(config))
            conf_file.close()
            if self.verbose:
                print 'created config.yml'
        except:
            pass
            #print 'failed to write config.yml'
        # sys.exit(1)
        #elif self.verbose:
        #    print 'config.yml already exists'


    def remove_config(self):
        config_path = path(self.out_path / self.hash_path)
        if Trial.CONFIG in self.connection.listdir(config_path):
            try:
                self.connection.exec_command('rm %s' %(config_path / Trial.CONFIG))
            except:
                print 'failed to remove config file'


    def submit(self):
        """
        Implementation dependant:
        Basically, call wrapper.py with
        self.out_path/self.out_dir/self.hash_path
        as the argument and make use of time, priority,
        and id whenever possible.
        """
        dir_ = self.out_path / self.hash_path
        path_ = self.wrap_path / path(Trial.WRAPPER)
        command = "%s %s" %(path_, dir_)
        #stdin, stdout, stderr = self.connection.exec_command('( ' + command + ' &)')
        #output = [x for x in stdout]
        #errors = [y for y in stderr]

        ssh_stdin, ssh_stdout, ssh_stderr = self.connection.exec_command('( ' + command + ' &)')
        if self.block:
            ssh_stdout.channel.recv_exit_status()
        return output, None


    def unsubmit(self):
        print 'unsubmit not yet supported here'


    def get_state(self):
        try:
            files = self.connection.listdir(self.out_path / self.hash_path)
        except:
            print  self.out_path / self.hash_path, ' not found'
            return None

        if '.finished' in files:
            return Trial.FINISHED
        elif '.error' in files:
            return Trial.ERROR
        else:
            if self.verbose:
                print 'in progress? ', files
            return None

    def freeze(self):
        return dict(
            out_path=self.out_path,
            exe_path=self.exe_path,
            params=self.params,
            time=self.time,
            priority=self.priority,
            verbose=self.verbose,
            test=self.test,
            server=self.get_server)

    def get_id(self):
        return self.id_


class CoalitionTrial(BaseTrial):
    _default_connection = None

    def get_server(self):
        return Trial.COALITION

    def __init__(self, env='coalition.yml', **kwargs):
        BaseTrial.__init__(self, env=env, **kwargs)


    def _get_default_connection(self):
        return CoalitionConnection(verbose=self.verbose)


    def submit(self):
        dir_ = self.out_path / self.hash_path
        if self.test:
            affin = 'test'
        else:
            affin = str(self.out_path.namebase)
        self.id_ = self.connection.run(
                         affinity=affin,
                         dir=self.exe_path.parent,
                         command='%s %s' %( self.wrap_path /
                                          path(Trial.WRAPPER), dir_) )
        # use id to get status and return (output, errors) for submission
        return list(), list()


    def unsubmit(self):
        print 'command unsubmit not supported yet.'


class SharcNetTrial(BaseTrial):
    _default_connection = None
    PATH = """/opt/kde3/bin:/opt/sharcnet/freepascal/current/bin:/opt/sharcnet/blcr/0.8.4/bin:/opt/sharcnet/gaussian/g09_C.01/bin:/opt/sharcnet/gaussian/g09_C.01:/opt/sharcnet/gaussian/g03_E.01/bin:/opt/sharcnet/gaussian/g03_E.01:/opt/sharcnet/lammps/10.08.2010/bin:/opt/sharcnet/r/2.10.0/bin:/opt/sharcnet/namd/2.7b3/bin:/opt/sharcnet/octave/3.2.4/snbin:/opt/sharcnet/ansys/13.0/snbin:/opt/sharcnet/ddt/3.1/bin:/opt/sharcnet/fftw/2.1.5/intel/bin:/opt/sharcnet/gromacs/4.0.5/bin:/opt/sharcnet/intel/11.0.083/icc/bin/intel64/:/opt/sharcnet/intel/11.0.083/ifc/bin/intel64/:/opt/sharcnet/sq-tm/2.4/bin:/opt/sharcnet/compile/1.3/bin:/opt/sharcnet/moab/6.1.3/bin:/opt/sharcnet/torque/2.5.12/bin:/opt/sharcnet/torque/2.5.12/sbin:/opt/sharcnet/torque/2.5.12/snbin:/opt/sharcnet/torque/2.5.12/manage:/opt/sharcnet/freepascal/current/bin:/usr/kerberos/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/sharcnet/blast/current/bin:/opt/sharcnet/openmpi/1.4.2/intel/bin:/opt/sharcnet/blast/current/bin:/home/cfobel/bin:
"""
    def get_server(self):
        return Trial.SHARCNET

    def __init__(self, env='sharcnet.yml', **kwargs):
        BaseTrial.__init__(self, env=env, **kwargs)


    def _get_default_connection(self):
        return SharcNetConnection(verbose=self.verbose)


    def submit(self):
        # set the PATH environment
        dir_ = self.out_path / self.hash_path
        if self.test:
            test = '--test '
        else:
            test = ''
        if test:
            print 'setting time to 60 mins for test'
            self.time = 60
        work_path = resolve(self.env, Trial.WORK)
        if self.verbose:
            print 'work path = ', work_path
        command = "LD_LIBRARY_PATH=%s/local/lib PATH=%s\n sqsub %s -r  %d -o %s %s %s %s" % (
                   work_path,
                   SharcNetTrial.PATH + ":/home/%s/bin" %self.connection.get_username(),
                   test, self.time, str(dir_/path(Trial.LOG)),
                   self.python_path,
                   str(self.wrap_path / path(Trial.WRAPPER)), str(dir_))
        if self.verbose:
            print 'Command = ', command
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



def trial_from_config(config_path, config_file, server):
    config = yaml.load(config_file)
    params = config[1]
    exe = config[0]
    config_path = path(config_path).abspath()
    output_dir = config_path.parent.parent

    if server == Trial.SHARCNET:
        return SharcNetTrial(out_path=output_dir, exe_path=exe, params=params)
    elif server == Trial.COALITION:
        return CoalitionTrial(out_path=output_dir, exe_path=exe, params=params)
    elif server == Trial.LOCAL:
        return BaseTrial(out_path=output_dir, exe_path=exe, params=params)
    else:
        raise ValueError('Unknown Server %s' % server)


