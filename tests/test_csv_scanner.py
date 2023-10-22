from io import StringIO, BytesIO
import logging
from unittest import TestCase

from csv_scanner import CSVScanner
from text_io_progress_wrapper import TextIOProgressWrapper

from data.test_data import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('csv_scanner')


class TestCsvScannerDataTypes(TestCase):

    def setUp(self) -> None:
        self.scanner = CSVScanner(open(csv_sources['microbe']), 'microbe')

    def test_that_decimals_tagged_as_decimals(self):
