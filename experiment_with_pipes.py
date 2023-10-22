#!/usr/bin/env python3

import os
from random import randint
from shutil import which
import sys
import tempfile
# import zipfile
from subprocess import Popen, PIPE, run



def main(db_path):
    x = run([which("sqlite3"), db_path], shell=False, stdin=PIPE, stdout=PIPE, stderr=PIPE, check=True)

    table_name = 'ab'
    tmpdir = tempfile.mkdtemp()
    fifo_fname = os.path.join(tmpdir, f"{table_name}_{randint(0, sys.maxsize)}")
    os.mkfifo(fifo_fname)

    x = run([which("sqlite3"), '-cmd', 'create table ab (a int, b text);', db_path], shell=False, stdin=PIPE, stdout=PIPE, stderr=PIPE, check=True)

    proc = Popen([
        which("sqlite3"),
        '-cmd', '.mode csv',
        '-cmd', '.separator \\t \\n',
        '-cmd', f'.import {fifo_fname} ab',
        db_path], stdin=PIPE, stdout=PIPE, stderr=PIPE)

    fifo_fh = open(fifo_fname, "wb")  # flags=os.O_WRONLY|os.O_NONBLOCK)
    tsv_fh = open("/home/kev/projs/quicknoot/temp_data.tsv", "rb")  # flags=os.O_RDONLY)
    fifo_fh.write(tsv_fh.read())
    print('hoo ha!')

def main_also_works(db_path):
    x = run([which("sqlite3"), '-cmd', 'create table ab (a int, b text);', db_path], shell=False, stdin=PIPE, stdout=PIPE, stderr=PIPE, check=True)
    x = run([
        which("sqlite3"),
        '-cmd', '.mode csv',
        '-cmd', '.separator \\t \\n',
        '-cmd', '.import /tmp/pipe ab',
        # /home/kev/projs/quicknoot/temp_data.tsv
        db_path],
        shell=False, stdin=PIPE, stdout=PIPE, stderr=PIPE, check=True)
    import pudb; pu.db
    z = 1


def main_worked_bang(db_path):
    x = run([which("sqlite3"), '-cmd', 'create table ab (a int, b text);', db_path], shell=False, stdin=PIPE, stdout=PIPE, stderr=PIPE, check=True)
    import pudb; pu.db
    z = 1

def main1(db_path):
    proc = Popen([which("sqlite3"), db_path], shell=False, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    proc.stdin.write(b"create table ab (a int, b text);\n")
    # print(proc.stdout.read())

    proc.stdin.write(b".tab\n")
    # print(proc.stdout.read())

    proc.stdin.write(b".quit\n")
    # print(proc.stdout.read())

    proc.stdin.flush()
    import pudb; pu.db
    x = proc.communicate()
    # proc.stdin.flush()
    # for line in proc.stdout.readlines():
    #     print(line)
    # proc.stdin.close()
    proc.wait()

if __name__ == '__main__':
    main("/tmp/test3.db")
