from io import StringIO, BytesIO
import logging
from unittest import TestCase

from text_io_progress_wrapper import TextIOProgressWrapper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('csv_scanner')

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
    def test_that_raises_from_missing_parameters(self):
        def sentinel(_, __, ___, ____):
            x = 1

        def raises_on_only_callback():
            wrapper = TextIOProgressWrapper(
                BytesIO(sample_data),
                object_name="fnoord",
                file_len=sample_data_len,
                callback=sentinel,
                # missing every_rows or every_pct
            )

        def raises_on_only_progress_fh():
            dev_null = open("/dev/null", "w")
            try:
                wrapper = TextIOProgressWrapper(
                    BytesIO(sample_data),
                    object_name="fnoord",
                    file_len=sample_data_len,
                    progress_fh=dev_null,
                    # missing every_rows or every_pct
                )
            except ValueError:
                raise
            finally:
                dev_null.close()

        def raises_on_only_every_rows():
            wrapper = TextIOProgressWrapper(
                BytesIO(sample_data),
                object_name="fnoord",
                file_len=sample_data_len,
                every_rows=1,
                # missing callback or progress_fh
            )

        def raises_on_only_every_pct():
            wrapper = TextIOProgressWrapper(
                BytesIO(sample_data),
                object_name="fnoord",
                file_len=sample_data_len,
                every_pct=0.1,
                # missing callback or progress_fh
            )

        def raises_on_both_every_pct_and_every_rows():
            dev_null = open("/dev/null", "w")
            try:
                wrapper = TextIOProgressWrapper(
                    BytesIO(sample_data),
                    object_name="fnoord",
                    file_len=sample_data_len,
                    progress_fh=dev_null,
                    every_pct=0.1,
                    every_rows=1,
                )
            except ValueError:
                raise
            finally:
                dev_null.close()

        def raises_on_both_callback_and_progress_fh():
            dev_null = open("/dev/null", "w")
            try:
                wrapper = TextIOProgressWrapper(
                    BytesIO(sample_data),
                    object_name="fnoord",
                    file_len=sample_data_len,
                    progress_fh=dev_null,
                    callback=sentinel,
                    every_pct=0.1,
                )
            except ValueError:
                raise
            finally:
                dev_null.close()

        def raises_on_all_options():
            dev_null = open("/dev/null", "w")
            try:
                wrapper = TextIOProgressWrapper(
                    BytesIO(sample_data),
                    object_name="fnoord",
                    file_len=sample_data_len,
                    progress_fh=dev_null,
                    callback=sentinel,
                    every_pct=0.1,
                    every_rows=1,
                )
            except ValueError:
                raise
            finally:
                dev_null.close()

        self.assertRaises(ValueError, raises_on_only_callback)
        self.assertRaises(ValueError, raises_on_only_progress_fh)
        self.assertRaises(ValueError, raises_on_only_every_rows)
        self.assertRaises(ValueError, raises_on_only_every_pct)
        self.assertRaises(
            ValueError, raises_on_both_every_pct_and_every_rows)
        self.assertRaises(
            ValueError, raises_on_both_callback_and_progress_fh)
        self.assertRaises(ValueError, raises_on_all_options)

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

    def test_that_callback_works_every_rows(self):
        _object_name = "fnoord"

        def callback_checker(object_name, line_num, char_num, file_len) -> None:
            line_pos = -1
            char_pos = 0
            limit = line_num

            while limit >= 0:
                pos = sample_data.find(b"\n", char_pos) + 1
                if pos > 0:
                    char_pos = pos
                else:
                    char_pos = len(sample_data)
                limit -= 1
                line_pos += 1

            self.assertEqual(object_name, _object_name)
            self.assertEqual(line_num, line_pos)
            self.assertEqual(char_num, char_pos)

        wrapper = TextIOProgressWrapper(
            BytesIO(sample_data),
            callback=callback_checker,
            object_name=_object_name,
            file_len=sample_data_len,
            every_rows=1,
        )

        while line := wrapper.readline():
            pass

    def test_that_callback_works_every_pct(self):
        _object_name = "fnoord"
        data = b"\n".join(b"_" * 100 for _ in range(100)) + b"\n"

        def callback_checker(object_name, line_num, char_num, file_len) -> None:
            logger.debug(
                (char_num / file_len * 100),
                (char_num / file_len * 100) - line_num,
                round(char_num / file_len * 100 - line_num),
                char_num, file_len, line_num)
            if line_num < 100:
                self.assertAlmostEqual(1, round(char_num / file_len * 100 - line_num), 0)
            else:
                # Because at EOF TextIOProgressWrapper makes a final call,
                # when char_num == file_len
                self.assertAlmostEqual(0, round(char_num / file_len * 100 - line_num), 0)

        wrapper = TextIOProgressWrapper(
            BytesIO(data),
            callback=callback_checker,
            object_name=_object_name,
            file_len=len(data),
            every_pct=0.01,
        )

        while line := wrapper.readline():
            pass

    def test_that_progress_fh_works(self):
        progress_fh = StringIO()
        _object_name = "fnoord"
        data = b"\n".join(b"_" * 100 for _ in range(100)) + b"\n"

        wrapper = TextIOProgressWrapper(
            BytesIO(data),
            progress_fh=progress_fh,
            object_name=_object_name,
            file_len=len(data),
            every_pct=0.01,
        )

        wrapper.read()
        progress_fh.seek(0)
        self.assertEqual(progress_fh.read(), '\rfnoord: row 99 of 99 - 100%')
