import itertools
import re
from subprocess import PIPE, Popen, check_call, check_output

from path import path
import yaml


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
    env_vars = yaml.load(path('/home/cfobel/env_vars.yml').bytes())
    resolve_env_vars(env_vars)
    print env_vars
