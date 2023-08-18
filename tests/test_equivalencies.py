from functools import partial
import re
from unittest import TestCase

from csv2db import zip_walker


zip_filename = 'tests/data/zips/main_and_two_subdir_csv.zip'
sources = [
    "data/csv/main_and_two_subdir_csv/subdir2/microbe.csv",
    "data/csv/main_and_two_subdir_csv/food_protein_conversion_factor.csv",
    "data/csv/main_and_two_subdir_csv/subdir1/measure_unit.csv",
]


class EquivalencyCases(TestCase):
    def test_that_zip_has_three_files(self):
        file_count = 0

        def count_files(_, _, table_name, csv_fh, file_info):
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
        self.assertEqual(file_count, len(sources))
