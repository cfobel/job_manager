#! /usr/bin/env python

import sys
from tables import *
from path import path

def _parse_args():
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('path', type=path)
    parser.add_argument('-print', dest='_print', action='store_true')
    parser.add_argument('-not', dest='_not', action='store_true')
    parser.add_argument('state', choices=['finished', 'logged', 'errored', 'running', 'corrupt'])
    return parser.parse_args()

class ResultsDirectory(object):
    def __init__(self, result_path):
        result_path = path(result_path)
        if not result_path.exists():
            raise ValueError(str(result_path) + 'Does not exist')
        else:
            self.path = result_path

    def _get_with(self, this):
        return  [f for f in self.get_all() if this in [x.name for x in f.files()]]

    def _complement(self, files):
        return list(set(self.get_all()).difference(set(files)))

    def get_all(self):
        return  [f.parent for f in self.path.walkfiles('config.yml')]

    def get_finished(self):
        return self._get_with('.finished')

    def get_logged(self):
        return self._get_with('log.txt')

    def get_errored(self):
        return self._get_with('.error')

    def get_running(self):
        return list(set(self.get_logged()).difference(set(self.get_finished()).union(self.get_errored())))

    def get_corrupt_hdf5(self):
        files = [f for f in j.walkfiles('*.h5') for j in self.get_finished()]
        corrupt = list()
        for f in files:
            try:
                t = openFile(f)
                t.root
            except:
                corrupt.append(f)
        return corrupt

    def get_uncorrupt_hdf5(self):
        files = [f for f in j.walkfiles('*.h5') for j in self.get_finished()]
        working = list()
        for f in files:
            try:
                t = openFile(f)
                t.root
                working.append(f)
            except:
                continue
        return working

if __name__ == "__main__":
    args = _parse_args()
    resultdir = ResultsDirectory(args.path)
    glob = getattr(resultdir, 'get_' + args.state)()
    if args._not:
        glob = resultdir._complement(glob)
    if args._print:
        for f in glob:
            print str(f)

