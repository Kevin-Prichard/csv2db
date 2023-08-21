#!/usr/bin/env python3

import argparse
import logging
import os
from functools import partial
from io import TextIOWrapper
import pickle
from random import randint
import re
from shutil import which
from subprocess import Popen, PIPE, run
import sys
import tempfile
from typing import Tuple, List
import zipfile
from zipfile import ZipInfo

from csv_scanner import CSVScanner
from text_io_progress_wrapper import TextIOProgressWrapper
from bytes_io_progress_wrapper import BytesIOProgressWrapper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('csv2db')

CSV_EXT_RX = re.compile(r'.*\.csv$')

std_err = sys.stderr


class ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        sys.stderr.write('error: %s\n' % message)
        self.print_help()


def get_args(argv: List[str]) -> Tuple[argparse.Namespace, ArgumentParser]:
    parser = ArgumentParser(
        prog='csv2db.py',
        description='CSV Schema Generator:'
                    'extracts the top -n rows from .CSV files '
                    'in a .ZIP archive.')
    parser.add_argument('--zip', '-z', dest='zip_file', type=str,
                        action='store')
    parser.add_argument('--sqlite', '-s', dest='sqlite_db_file', type=str,
                        action='store')
    parser.add_argument('--filter', '-f', dest='name_filter', type=str,
                        action='store')
    parser.add_argument(
        '--create-only', '-c', dest='create_only', action='store_const',
        default=False, const=True,
        help='Create tables in target db, then exit')
    parser.add_argument(
        '--show-struct', '-w', dest='show_struct', action='store_const',
        default=False, const=True,
        help='Show scan statistics & table structure then exit')
    parser.add_argument(
        '--save-struct', '-t', dest='save_struct', action='store',
        default=None,
        help='Filename to write scan statistics & table structure, then exit')
    parser.add_argument('--max', '-n', dest='max_csv_rows', default=0,
                        type=int, action='store', nargs='?')
    parser.add_argument(
        '--progress-rows', '-r', dest='every_rows', default=None, type=int,
        help='Show progress update every -r rows')
    parser.add_argument(
        '--progress-pct', '-p', dest='every_pct', default=None, type=float,
        help='Show progress update every -p percent of a csv file')
    return parser.parse_args(argv), parser


def zip_walker(zip_filename,
               name_filter: re.Pattern = None,
               max_rows=None,
               output_fn=None,
               create_only=False,
               every_rows=None,
               every_pct=None,
               show_struct=False,
               save_struct=None,
               ):

    with zipfile.ZipFile(zip_filename, "r") as zip:
        table_sql = dict()
        if save_struct:
            structs = {}

        for file_no, name in enumerate(zip.namelist()):
            if name_filter and not name_filter.match(name):
                continue
            table_name = os.path.basename(name).split(".")[0]
            file_info = zip.getinfo(name)
            file_len = file_info.file_size

            if CSV_EXT_RX.match(name):
                with zip.open(name) as csv_fh:
                    if show_progress:=every_rows or every_pct:
                        csv_fh = TextIOProgressWrapper(
                            csv_fh,
                            object_name=name,
                            file_len=file_len,
                            progress_fh=std_err,
                            every_rows=every_rows,
                            every_pct=every_pct,
                        )
                    else:
                        csv_fh = TextIOWrapper(csv_fh)

                    ss = CSVScanner(
                        csv_fh,
                        table_name,
                        file_len=file_len,
                        max_rows=max_rows,
                    )
                    ss.scan()
                    if show_progress:
                        std_err.write("\n")
                if show_struct:
                    buf = []
                    for fname, stats in ss.stats.items():
                        buf.append(
                            f"    {fname}: "
                            f"{', '.join([f'{typ}:{cnt}' for typ, cnt in stats.items()])}")
                    print("Statistics\n", "\n".join(buf))
                    print("Decision\n", list(ss.result()))
                    print("Table structure\n", ss.sql_create_table())

                if save_struct:
                    structs[table_name] = {
                        fname: f"{typ}{f'({var_size})' if var_size else ''}"
                        for fname, typ, var_size in ss.result()
                    }

                if output_fn and not (show_struct or save_struct):
                    with zip.open(name) as csv_fh:
                        if show_progress := every_rows or every_pct:
                            csv_fh = BytesIOProgressWrapper(
                                source=csv_fh,
                                object_name=name,
                                every_rows=every_rows,
                                every_pct=every_pct,
                                progress_fh=std_err,
                                file_len=file_len,
                            )
                        _ = csv_fh.readline()

                        output_fn(
                            scanner=ss,
                            table_name=table_name,
                            csv_fh=csv_fh,
                            file_info=file_info,
                            create_only=create_only,
                        )
                        if show_progress:
                            std_err.write("\n")
                else:
                    table_sql[table_name] = ss.sql_create_table()

    if save_struct:
        with open(save_struct, "wb") as struct_fh:
            a = pickle.dumps(structs)
            struct_fh.write(a)

    if not output_fn:
        return table_sql


def create_import_sqlite(
        db_path, scanner: CSVScanner, table_name, csv_fh,
        file_info: ZipInfo,
        create_only: bool = False):
    """
    :param db_path:  - sqlite path
    :param scanner:  - instance of CSVScanner
    :param table_name:  - name of table we're creating
    :param csv_fh:   - file handle of delimited data source
    :param file_info:   - ZipInfo instance
    :param create_only: - bool, only create tables, don't import
    :return:

    Must follow this order of operations:
    Create FIFO path, create table, start import to table from FIFO, then push data down FIFO

    Currently-
    1. Create FIFO pathname using tempfile.mkdtenp and a random number
    2. Create table in Sqlite db
    3. Start sqlite import to table, piping input from FIFO, which blocks until...
    4. Open the FIFO as a file
    5. Read csv_fh and write it to FIFO, until complete
    6. Send '.quit' to Sqlite3
    7. Close 'proc' of Sqlite3
    8. Close FIFO, csv_fh
    """

    proc, fifo_fname, tmpdir = None, None, None
    try:
        tmpdir = tempfile.mkdtemp()
        fifo_fname = os.path.join(
            tmpdir, f"{table_name}_{hex(randint(0, sys.maxsize))[2:]}")
        os.mkfifo(fifo_fname)
        logger.debug("mkfifo %s", fifo_fname)
    except OSError as e:
        logger.exception("Failed to create FIFO: %s", e)
    else:
        run([
            which("sqlite3"),
            '-cmd', scanner.sql_create_table_1line(),
            db_path
        ], stdin=PIPE, stdout=PIPE, stderr=PIPE, check=True)
        logger.debug("sqlite3: created table %s", table_name)

        if create_only:
            return

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
                        fifo_fh, csv_fh, file_info, 2 ** buf_size_power)
                    logger.debug("Power that worked: %d", buf_size_power)
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
        logger.debug("create_import_sqlite: csv_fh close")

        # Must always directly .quit sqlite3
        # otherwise it hangs around and interferes with subsequent imports
        if proc:
            logger.debug("create_import_sqlite: sqlite3 .quit/flush/close")
            proc.stdin.write(b".quit\n")
            try:
                proc.stdin.flush()
                proc.stdin.close()
            except BrokenPipeError:
                pass
            finally:
                proc.wait()
            logger.debug("create_import_sqlite: sqlite3 .quit/flush/close done")

        if fifo_fname:
            logger.debug("create_import_sqlite: rm fifo_fname %s", fifo_fname)
            os.remove(fifo_fname)
            logger.debug("create_import_sqlite: rm fifo_fname %s done",
                         fifo_fname)
        if tmpdir:
            logger.debug("create_import_sqlite: rmdir tmpdir %s", tmpdir)
            os.rmdir(tmpdir)
            logger.debug("create_import_sqlite: rmdir tmpdir %s done", tmpdir)


def transfer(fifo_fh, csv_fh, file_info, buf_size):
    left = file_info.file_size
    while left > 0:
        can_do = min(left, buf_size)
        try:
            logger.debug("write %d bytes to fifo, left: %d", buf_size, left)
            fifo_fh.write(csv_fh.read(can_do))
            fifo_fh.flush()
            left -= can_do
            logger.debug("  wrote %d bytes to fifo, left: %d", buf_size, left)
        except Exception as e:
            logger.exception("Failed on exception: %s", str(e), 3)
            raise e
        except BrokenPipeError as bpe:
            logger.exception("Failed on buf_size %d" % buf_size, bpe)
            raise bpe

        if left <= 0:
            return True


def main(argv=None):
    if argv is None:
        argv = sys.argv
    args, parser = get_args(argv)
    create_fn, name_filter = None, None
    if args.sqlite_db_file:
        create_fn = partial(create_import_sqlite, args.sqlite_db_file)
    if args.name_filter:
        name_filter = re.compile(args.name_filter, re.I)
    if args.zip_file:
        zip_walker(
            zip_filename=args.zip_file,
            name_filter=name_filter,
            create_only=args.create_only,
            show_struct=args.show_struct,
            save_struct=args.save_struct,
            max_rows=args.max_csv_rows,
            output_fn=create_fn,
            every_rows=args.every_rows,
            every_pct=args.every_pct,
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main(sys.argv[1:])
