import itertools
import re
from subprocess import PIPE, Popen, check_call, check_output

from path import path
import yaml


def resolve(env_vars, path_):
    resolve_env_vars(env_vars)
    cre_env = re.compile(r'\$\{(.*?)\}')
    key_env = re.compile(r'\$\{(?P<key>.*)\}', re.X)
    match = cre_env.search(path_)
    while match:
        m = match.group()
        k = key_env.match(m).groupdict()['key']
        if k not in env_vars:
            raise ValueError('Missing Value for symbol %s' %m)
        if isinstance(env_vars[k], list):
            repl = ':'.join(env_vars[k])
        else:
            repl = env_vars[k]
        path_ = str.replace(path_, m, repl)
        match = cre_env.search(path_)
    return path(path_)

def resolve_path(env_vars, path_):
    if env_vars:
        resolve_env_vars(env_vars)
        did_replace = True

        for pure in env_vars.keys():
            if isinstance(env_vars[pure], list):
                path_ = re.sub(pattern=r'\$%s' % pure, repl=':'.join(env_vars[pure]),
                string=path_) 
            else:
                path_ = re.sub(pattern=r'\$%s' % pure, repl=env_vars[pure],
                string=path_) 
            
            path_ = re.sub(pattern=r'//', repl='/', string=path_)        


        return path(path_)
    else:
        return path(path_)


def resolve_env_vars(env_vars):
    cre_env = re.compile(r'\$\{(.*?)\}')

    unresolved = set(itertools.chain(*[cre_env.findall(v) for var_list in env_vars.values() for v in var_list if cre_env.search(v)]))

    while unresolved:
        pure_vars = [var_name for var_name, vars in [(k, env_vars[k]) for k in unresolved] for v in vars if cre_env.search(v) is None]
        for var_name, values in env_vars.items():
            for i in range(len(values)):
                for pure_var in pure_vars:
                    values[i] = re.sub(r'\$\{%s\}' % pure_var, env_vars[pure_var][0], values[i])
        unresolved = set(itertools.chain(*[cre_env.findall(v) for var_list in env_vars.values() for v in var_list if cre_env.search(v)]))


if __name__ == '__main__':
    env_vars = yaml.load(path('/home/rpattiso/job_manager/environments/env_vars.yml').bytes())
    print env_vars
    resolve_env_vars(env_vars)
    print 'resolve'
    print env_vars
    path_ = resolve(env_vars, '${PYVPR_EXAMPLES}96/freeze_annealer')
    print path_


