from io import BytesIO
from unittest import TestCase

from text_io_progress_wrapper import TextIOProgressWrapper

sample_data = b"""Line one
Line two
Line three"""

sample_data_lines = sample_data.split(b"\n")
sample_data_len = len(sample_data)
sample_data_line_count = len(sample_data.split(b"\n"))


class TestTextIOProgressWrapperCountersPassThru(TestCase):
    def setUp(self) -> None:
        self.wrapper = TextIOProgressWrapper(
            BytesIO(sample_data),
            object_name='_',
            file_len=sample_data_len,
            every_rows=1,
        )

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


class TestTextIOProgressWrapperCallbacks(TestCase):
    def test_that_doesnt_callback_without_every(self):
        def sentinel(_, __, ___, ____):
            pass

        def this_will_throw_ValueError():
            wrapper = TextIOProgressWrapper(
                BytesIO(sample_data),
                callback=sentinel,
                object_name="fnoord",
                file_len=sample_data_len,
            )

        self.assertRaises(ValueError, this_will_throw_ValueError)

    def test_that_does_callback_with_file_len(self):
        was_called_back = False

        def sentinel(_, __, ___, ____):
            nonlocal was_called_back
            was_called_back = True

        wrapper = TextIOProgressWrapper(
            BytesIO(sample_data),
            callback=sentinel,
            object_name="xyzzy",
            file_len=len(sample_data),
            every_rows=1,
        )

        wrapper.read()
        self.assertTrue(was_called_back)

    def test_that_callback_works(self):
        line_count = 0
        char_pos = 0
        _object_name = "fnoord"

        def callback_checker(object_name, line_num, char_num, file_len) -> None:
            nonlocal line_count, char_pos
            line_count += 1
            char_pos = sum(len(sample_data_lines[i]) + 1
                           for i in range(len(sample_data_lines))
                           if i <= line_num)
            self.assertEqual(object_name, _object_name)
            self.assertEqual(line_num, line_count)
            self.assertEqual(char_num, char_pos)

        wrapper = TextIOProgressWrapper(
            BytesIO(sample_data),
            callback=callback_checker,
            object_name=_object_name,
            file_len=sample_data_len,
            every_rows=1,
        )

        while line := wrapper.readline():
            char_pos += len(line)
