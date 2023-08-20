from io import BytesIO, BufferedIOBase
from typing import List, Tuple, Callable

from bytes_io_stats_wrapper import BytesIOStatsWrapper


BytesIOProgressCallback = Callable[[str, int, int, int], None]


class BytesIOProgressWrapper(BytesIOStatsWrapper):
    def __init__(self,
                 source: BufferedIOBase,
                 file_len,
                 object_name,
                 every_rows=None,
                 every_pct=None,
                 callback: BytesIOProgressCallback = None,
                 progress_fh=None,
                 **kw):
        super().__init__(source=source)
        outputting = any((callback, progress_fh))
        output_both = all((callback, progress_fh))
        everying = any((every_rows, every_pct))
        every_both = all((every_rows, every_pct))

        if outputting and not everying:
            raise ValueError(
                "When using 'callback' or 'progress_fh' you "
                "need to specify either 'every_rows' or "
                "'every_pct' to tell how often to report on progress")
        if everying and not outputting:
            raise ValueError(
                "When specifying 'every_rows' or 'every_pct' you also need "
                "to specify 'callback' or 'progress_fh'")
        if output_both:
            raise ValueError("Specify either 'callback' or 'progress_fh' "
                             "but not both")
        if every_both:
            raise ValueError("Specify either 'every_row' or 'every_pct' "
                             "but not both")

        self._object_name = object_name
        self._every_rows = every_rows
        self._every_pct = every_pct
        self._file_len = file_len
        self._callback = callback
        self._progress_fh = progress_fh
        self._last_indication = 0

    def _check_progress(self):
        do_progress = False
        rows_est = self._file_len / (self.char_num / max(1, self.line_num))
        if self._every_rows:
            if do_progress := (
                    self.line_num >= self._last_indication + self._every_rows or
                    self.char_num >= self._file_len - 1):
                self._last_indication = self.line_num
        elif self._every_pct:
            if do_progress := (
                    ((pct := int(
                        self.char_num / self._file_len * 100))
                     > self._last_indication)
                    or self.char_num >= self._file_len):
                self._last_indication = pct
        if do_progress:
            if self._callback:
                self._callback(self._object_name, self.line_num,
                               self.char_num, self._file_len)
            elif self._progress_fh:
                self._progress_fh.write(
                    f"\r{self._object_name}: "
                    f"row {self.line_num:,}"
                    f" of {int(rows_est):,} - "
                    f"{int(self.char_num / self._file_len * 100)}%")

    def read(self, *args, **kw):
        data = super().read(*args, **kw)
        self._check_progress()
        return data

    def readline(self, limit=None):
        data = super().readline(limit)
        self._check_progress()
        return data

    def readlines(self, hint=-1) -> List[bytes]:
        data = super().readlines(hint)
        self._check_progress()
        return data
