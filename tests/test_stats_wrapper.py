from io import BytesIO
from unittest import TestCase

from text_io_stats_wrapper import TextIOStatsWrapper


sample_data = b"""Line one
Line two
Line three"""

sample_data_len = len(sample_data)
sample_data_line_count = len(sample_data.split(b"\n"))


class TestIOStatsWrapper(TestCase):
    def setUp(self) -> None:
        self.wrapper = TextIOStatsWrapper(BytesIO(sample_data))

    def test_that_stats_counts_chars_correctly(self):
        self.wrapper.read()
        self.assertEqual(self.wrapper.char_num, sample_data_len)

    def test_that_stats_counts_lines_correctly(self):
        while self.wrapper.readline():
            pass
        self.assertEqual(self.wrapper.line_num, sample_data_line_count)

    def test_that_stats_returns_same_data(self):
        self.assertEqual(
            self.wrapper.read(),
            sample_data.decode('utf-8'))

    def test_that_stats_returns_same_lines(self):
        self.assertEqual(
            "".join(self.wrapper.readlines()),
            sample_data.decode('utf-8'))

