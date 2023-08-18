from functools import partial
import re
from unittest import TestCase

from csv_scanner import CSVScanner
from csv2db import zip_walker

zip_filename = 'tests/data/zips/main_and_two_subdir_csv.zip'
sources = [
    "data/csv/main_and_two_subdir_csv/subdir2/microbe.csv",
    "data/csv/main_and_two_subdir_csv/food_protein_conversion_factor.csv",
    "data/csv/main_and_two_subdir_csv/subdir1/measure_unit.csv",
]
structures = {
    "food_protein_conversion_factor": [
        ("food_nutrient_conversion_factor_id", "int", None),
        ("value", "int", None)
    ],
    "measure_unit": [
        ('id', 'int', None),
        ('name', 'str', 16)
    ],
    "microbe": [
        ('id', 'int', None),
        ('foodId', 'int', None),
        ('method', 'str', 19),
        ('microbe_code', 'str', 28),
        ('min_value', 'int', None),
        ('max_value', 'str', 0),
        ('uom', 'str', 5),
    ]
}


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
        self.assertEqual(file_count, len(sources))

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
