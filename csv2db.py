#!/usr/bin/env python3

import argparse
import csv
import io
import logging
import os
from functools import partial
from random import randint
import re
from shutil import which
import sys
import tempfile
import zipfile
from subprocess import Popen, PIPE, run
from typing import Tuple, List

from csv_scanner import CSVScanner


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('csv2db')

CSV_EXT_RX = re.compile(r'.*\.csv$')


class ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        sys.stderr.write('error: %s\n' % message)
        self.print_help()


def get_args(argv: List[str]) -> Tuple[argparse.Namespace, ArgumentParser]:
    parser = ArgumentParser(
        prog='csv2db.py',
        description = 'CSV Schema Generator:'
                      'extracts the top -n rows from .CSV files in a .ZIP archive.')
    parser.add_argument('--file', '-f', dest='file', type=str, action='store')
    parser.add_argument('--dir', '-d', dest='output_dir', type=str, action='store')
    parser.add_argument('--limit', '-l', dest='max_csv_files', default=0, type=int, action='store', nargs='?')
    parser.add_argument('--max', '-n', dest='max_csv_rows', default=0, type=int, action='store', nargs='?')
    parser.add_argument('--sqlite', '-s', dest='sqlite_db_file', type=str, action='store')
    return parser.parse_args(argv), parser


def get_table_lengths(table_len_csv_fh):
    rdr = csv.reader(io.TextIOWrapper(table_len_csv_fh, encoding='utf-8'))
    next(rdr)
    return {row[0]: int(row[1]) for row in rdr}


def zip_walker(zip_filename, output_dir, max_files, max_rows, output_fn):
    with zipfile.ZipFile(zip_filename, "r") as zip:
        zip_name = os.path.basename(zip_filename)
        files_done = 0
        table_lengths_filename = "all_downloaded_table_record_counts.csv"
        table_lengths = get_table_lengths(zip.open(table_lengths_filename))
        table_sql = dict()
        for file_no, name in enumerate(zip.namelist()):
            table_name = name.split(".")[0]
            # if table_name not in ('acquisition_samples', 'fndds_derivation'):
            #     continue
            # if max_files and file_no >= max_files:
            #     break
            if CSV_EXT_RX.match(name) and name != table_lengths_filename:
                with zip.open(name) as csv_file:
                    ss = CSVScanner(
                        io.TextIOWrapper(csv_file, encoding='utf-8'),
                        table_name,
                        table_lengths,
                        max_rows=max_rows,
                    )
                    ss.scan()
                if output_fn:
                    with zip.open(name) as csv_file:
                        output_fn(ss, table_name, csv_file, file_info=zip.getinfo(name))
                else:
                    table_sql[table_name] = ss.sql_create_table()
    if not output_fn:
        return table_sql


def create_import_sqlite(db_path, scanner: CSVScanner, table_name, csv_fh, file_info):
    try:
        tmpdir = tempfile.mkdtemp()
        fifo_fname = os.path.join(
            tmpdir, f"{table_name}_{hex(randint(0, sys.maxsize))[2:]}")
        os.mkfifo(fifo_fname)
        logger.debug("mkfifo %s", fifo_fname)
    except OSError as e:
        logger.exception("Failed to create FIFO: %s", e)
    else:
        x = run([
            which("sqlite3"),
            '-cmd', scanner.sql_create_table_1line(),
            db_path
        ], stdin=PIPE, stdout=PIPE, stderr=PIPE, check=True)
        logger.debug("sqlite3: create table %s", table_name)

        proc = Popen([
            which("sqlite3"),
            '-cmd', '.mode csv',
            '-cmd', '.separator , \\n',
            '-cmd', f'.import {fifo_fname} {table_name}',
            db_path,
            '>', '/tmp/sout.log',
            '2>', '/tmp/serr.log',
            ], stdin=PIPE, stdout=PIPE, stderr=PIPE)
        logger.debug("sqlite3: start blocking pipe import %s", table_name)

        buf_size_power = 24  # 16MB
        while buf_size_power > 0:
            with open(fifo_fname, "wb") as fifo_fh:
                logger.debug("open fifo for write: %s", fifo_fname)
                csv_fh.seek(0)
                logger.debug("csv seek(0): %s", table_name)

                try:
                    finished = transfer(
                        fifo_fh, csv_fh, file_info, 2**buf_size_power)
                    logger.info("Power that worked: %d", buf_size_power)
                    break
                except BrokenPipeError as ee:
                    logger.warning("Power that failed: %d; backing off by 1",
                                   buf_size_power)
                    buf_size_power -= 1

        logger.debug("csv -> fifo, %s -> %s, completed: %d",
                     table_name, fifo_fname, )

    finally:
        if csv_fh:
            csv_fh.close()
            del csv_fh
        logger.info("create_import_sqlite: csv_fh close")

        # Must always directly .quit sqlite3
        # otherwise it hangs around and interferes with subsequent imports
        if proc:
            logger.info("create_import_sqlite: sqlite3 .quit/flush/close")
            proc.stdin.write(b".quit\n")
            proc.stdin.flush()
            proc.stdin.close()
            proc.wait()
            logger.info("create_import_sqlite: sqlite3 .quit/flush/close done")

        if fifo_fname:
            logger.info("create_import_sqlite: rm fifo_fname %s", fifo_fname)
            os.remove(fifo_fname)
            logger.info("create_import_sqlite: rm fifo_fname %s done", fifo_fname)
        if tmpdir:
            logger.info("create_import_sqlite: rmdir tmpdir %s", tmpdir)
            os.rmdir(tmpdir)
            logger.info("create_import_sqlite: rmdir tmpdir %s done", tmpdir)


def transfer(fifo_fh, csv_fh, file_info, buf_size):
    left = file_info.file_size
    while left > 0:
        can_do = min(left, buf_size)
        try:
            logger.info("write %d bytes to fifo, left: %d", buf_size, left)
            fifo_fh.write(csv_fh.read(can_do))
            fifo_fh.flush()
            left -= can_do
            logger.info("  wrote %d bytes to fifo, left: %d", buf_size, left)
        except BrokenPipeError as bpe:
            print("Failed on buf_size %d" % buf_size)
            raise bpe

        if left <= 0:
            return True


def main(argv=None):
    if argv is None:
        argv = sys.argv
    args, parser = get_args(argv)
    create_fn = None
    if args.sqlite_db_file:
        create_fn = partial(create_import_sqlite, args.sqlite_db_file)
    if args.file:
        zip_walker(args.file, args.output_dir, args.max_csv_files,
                   args.max_csv_rows, create_fn)
    else:
        parser.print_help()


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except Exception as ee:
        print(ee)
