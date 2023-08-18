from collections import defaultdict as dd
import csv
import logging
import re
import sys
from typing import Callable

from text_io_stats_wrapper import TextIOStatsWrapper

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
    def __init__(self, csv_fh,
                 table_name: str,
                 file_len: int = None,
                 max_rows: int = None,
                 report_cb: Callable = None,
                 report_cb_freq: float = None  # n>1 every n rows; 0<n<1 every n % chars
                 ):
        self._csv_fh = csv_fh
        self._table_name = table_name
        self._file_len = file_len
        self._report_cb = report_cb
        self._report_cb_freq = report_cb_freq
        self._report_cb_type = None
        if report_cb_freq is not None:
            self._report_cb_type = 'pct' if 0<report_cb_freq<1 else 'cnt'
            self._report_cb_freq = report_cb_freq * 100
        self._stats = dd(lambda: dd(int))
        self._str_max_len = dd(int)
        self._max_rows = max_rows

    def destroy(self):
        self._csv_fh.close()
        del self._table_name
        del self._file_len
        del self._stats
        del self._str_max_len
        del self._csv_fh
        del self._report_cb
        del self._report_cb_freq
        del self._max_rows

    def scan(self):
        stats_reader = TextIOStatsWrapper(self._csv_fh)
        reader = csv.reader(stats_reader)
        field_names = next(reader)
        last_indication = -1
        for row in reader:
            row_num = stats_reader.line_num
            rows_est = self._file_len / (stats_reader.char_num / row_num)
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
                if (self._report_cb_type == 'pct' and
                    (((pct := int(stats_reader.char_num / self._file_len * 100))
                        > last_indication)
                     or stats_reader.char_num >= self._file_len)):
                    std_err.write(
                        f"\r{self._table_name}: "
                        f"{pct}% ({stats_reader.char_num:,}"
                        f" of {self._file_len:,})")
                    last_indication = pct
                elif (self._report_cb_type == 'cnt' and (
                  row_num > last_indication) or
                      stats_reader.char_num >= self._file_len):
                    std_err.write(
                        f"\r{self._table_name}: "
                        f"row {row_num:,}"
                        f" of {rows_est:,}")
                    last_indication = row_num

            if row_num >= rows_est:
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
