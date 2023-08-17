import csv
from collections import defaultdict as dd
import logging
import re
import sys

std_err = sys.stderr

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('csv_scanner')


type_rx = re.compile(
    r"^(?P<date>\d{4}[\-/]\d{2}[\-/]\d{2})"
    r"|(?P<datetime>\d{4}[\-/]\d{2}[\-/]\d{2}.\d{2}:\d{2}:\d{2})"
    r"|(?P<time>\d{2}:\d{2}:\d{2})"
    r"|(?P<int>[\-+0-9]+)"
    r"|(?P<decimal>[\-+]?[0-9]\.?[0-9]*)"
    r"|(?P<bool>true|false)"
    r"|(?P<none>None)"
    r"|(?P<str>.*)$"
)


sql_type_conv = {
    "date": "DATE",
    "datetime": "DATETIME",
    "time": "TIME",
    "int": "INTEGER",
    "decimal": "REAL",
    "bool": "BOOLEAN",
    "none": "NULL",
    "str": "VARCHAR",
}


class CSVScanner:
    def __init__(self, csv_fh, table_name, table_lengths, max_rows=None):
        self._csv_fh = csv_fh
        self._table_name = table_name
        self._table_lengths = table_lengths
        self._stats = dd(lambda: dd(int))
        self._str_max_len = dd(int)
        self._max_rows = max_rows

    def destroy(self):
        self._csv_fh.close()
        del self._table_name
        del self._stats
        del self._str_max_len
        del self._csv_fh
        del self._table_lengths
        del self._max_rows

    def scan(self):
        reader = csv.reader(self._csv_fh)
        field_names = next(reader)
        num_rows = min(self._max_rows, self._table_lengths[self._table_name]) \
            if self._max_rows else self._table_lengths[self._table_name]
        last_pct = -1
        for row_num, row in enumerate(reader):
            for i, val in enumerate(row):
                tx = type_rx.match(val)
                types = {typ for typ, value in tx.groupdict().items()
                         if value is not None}
                if len(types) != 1:
                    logger.warning("Could not grok field '%s': %s",
                                   field_names[i], str(val))
                the_type = types.pop()
                self._stats[field_names[i]][the_type] += 1
                if the_type == 'str':
                    self._str_max_len[field_names[i]] = max(
                        self._str_max_len[field_names[i]], len(val))
                if ((pct:=int(row_num / num_rows * 100)) > last_pct
                        or row_num >= num_rows):
                    std_err.write(
                        f"\r{self._table_name}: "
                        f"{pct}% ({row_num:,} of {num_rows:,})")
                    last_pct = pct
            if row_num >= num_rows:
                break
        std_err.write("\n")

    def result(self):  # sourcery skip: assign-if-exp
        for field_name, typ in self._stats.items():
            typ_keys = typ.keys()
            the_typ = list(typ_keys)[0] if len(typ_keys) == 1 \
                else sorted(typ_keys, key=lambda key: typ[key], reverse=True)[0]
            if the_typ != 'str' and 'str' in typ_keys:
                the_typ = 'str'  # str always wins, means impure vals exist
            if len(typ_keys) != 1:
                std_err.write(
                    "Got multiple types for %s.%s: %s (went with %s)\n" % (
                    self._table_name, field_name, str(typ), the_typ)
                )
            if the_typ == 'str':
                str_len = self._str_max_len[field_name]
            else:
                str_len = None
            yield field_name, the_typ, str_len

    def __str__(self):  # sourcery skip: for-append-to-extend, list-comprehension
        buffer = []
        for field_name, the_typ, str_len in self.result():
            buffer.append(f'{field_name} {the_typ}'
                          f'{f" ({str(str_len)})" if str_len else ""}')
        buffer.append("")
        return "\n".join(buffer)

    def sql_create_table(self):  # sourcery skip: for-append-to-extend, list-comprehension, merge-list-appends-into-extend
        buffer = [f"CREATE TABLE {self._table_name} ("]
        field_defs = []
        for field_name, the_typ, str_len in self.result():
            field_defs.append(f'{field_name} {sql_type_conv[the_typ]}'
                  f'{f"({str(str_len)})" if str_len else ""}')
        buffer.append("    " + (",\n    ".join(field_defs)))
        buffer.append(');\n')
        return "\n".join(buffer)

    def sql_create_table_1line(self):
        return self.sql_create_table().replace('\n', ' ')