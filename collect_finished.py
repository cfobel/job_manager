#!/usr/bin/env python

from path import path
import sys

usage=\
"""
   ./collect_finished.py  directory_root  output_directory

    directory root is searched recursively to find folders/leaves containing .finished file.
    The leave folders with the .finished files are copied into the output directory if they
    do not already exist in that directory.
"""

def finished_paths(directory_root, ignore=[]):
    finished = []
    for dir_ in directory_root.walkdirs():
        if '.finished' in [f.namebase for f in dir_.files()]:
            if dir_.namebase not in ignore:
                finished.append(dir_)
    return finished


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print usage
        sys.exit(0)

    directory_root = path(sys.argv[1])
    output_directory = path(sys.argv[2])

    if not output_directory.exists():
        output_directory.mkdir()

    collected = [d.namebase for d in output_directory.walkdirs()]
    new = finished_paths(directory_root, ignore=collected)
    for dir_ in new:
        dir_.copytree(output_directory / dir_.namebase)
    print 'Collected %d Folders.' % len(new)
