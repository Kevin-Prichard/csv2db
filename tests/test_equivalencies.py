from functools import partial
import re
from unittest import TestCase

from csv_scanner import CSVScanner
from csv2db import zip_walker

from data.test_data import *


class EquivalencyCases(TestCase):
    def test_that_zip_has_three_files(self):
        file_count = 0

        def count_files(db_path, scanner: CSVScanner,
                        table_name, csv_fh, file_info):
            nonlocal file_count
            if table_name and csv_fh and file_info:
                file_count += 1

        def test():
            zip_walker(
                zip_filename=zip_filename,
                name_filter=re.compile(r".*\.csv$", re.I),
                output_fn=count_files2)

        count_files2 = partial(count_files, db_path=None)
        test()
        self.assertEqual(file_count, len(csv_sources))

    def test_that_zipped_csvs_have_expected_structure(self):

        def check_structure(db_path, scanner: CSVScanner,
                            table_name, csv_fh, file_info):
            struct = list(scanner.result())
            self.assertEqual(structures[table_name], struct)

        def test():
            zip_walker(
                zip_filename=zip_filename,
                name_filter=re.compile(r".*\.csv$", re.I),
                output_fn=check_structure2)

        check_structure2 = partial(check_structure, db_path=None)
        test()
