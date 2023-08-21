#!/usr/bin/env python3

import argparse
from collections import defaultdict as dd
import pickle
import sys
from typing import List


class ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        sys.stderr.write('error: %s\n' % message)
        self.print_help()


def get_args(argv: List[str]) -> argparse.Namespace:
    parser = ArgumentParser(
        prog='choose_key_fields.py',
        description='Locate likely key fields in csv2db --save-struct '
                    'dumpfiles. Compares first columns with other tables'
                    'referents of the same name, and recommends '
                    'those as foreign key pairs.')
    parser.add_argument('--file', '-f', dest='structs_file', type=str,
                        action='store')
    return parser.parse_args(argv)


TNAME = 0
COL_KEY = 1

def get_by_pos(table_list, position):
    # import pudb; pu.db
    res = list(filter(
        lambda tname_col: tname_col[COL_KEY] == position, table_list))
    if not res:
        # import pudb; pu.db
        return None, None
    return res[0]


def mk_inverted_index(structs):
    iindex = dd(set)
    for tname, tstruct in structs.items():
        for col_num, (fname, ftyp) in enumerate(tstruct.items()):
            iindex[(fname, ftyp)].add((tname, col_num))
    return iindex


def locate_pairs(iindex, structs):
    for fld, occurrences in iindex.items():
        if len(occurrences) == 1:
            # print("Only 1 occurrence: ", fld, occurrences)
            continue
        if len(occurrences) > 1:
            foreign_key, foreign_type = get_by_pos(occurrences, 0)
            referent_key, referent_type = get_by_pos(occurrences, 1)
            print(f"\n{len(occurrences)} occurrences of '{fld}': ",
                  foreign_key, foreign_type,
                  referent_key, referent_type)
            print(occurrences)
            # import pudb; pu.db

#        for tname, col_num in occurrences:


def scan_structs(struct):
    import pudb; pu.db
    for candidate, candidate_fields in struct.items():
        for referent, referent_fields in struct.items():
            for cfname, ctyp in candidate_fields:
                if cfname in referent_fields:
                    pass


def main(args):
    with open(args.structs_file, "rb") as fh:
        structs = pickle.loads(fh.read())
        iindex = mk_inverted_index(structs)
        pairs = locate_pairs(iindex, structs)


if __name__ == '__main__':
    main(get_args(sys.argv[1:]))
    # F=open("/tmp/fdc_structs.pkl", "rb")
