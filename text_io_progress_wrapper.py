from typing import List, Tuple, Callable

from text_io_stats_wrapper import TextIOStatsWrapper


TextIOProgressCallback = Callable[[str, int, int, int], None]


class TextIOProgressWrapper(TextIOStatsWrapper):
    def __init__(self, *args,
                 file_len,
                 object_name,
                 every_rows=None,
                 every_pct=None,
                 callback: TextIOProgressCallback = None,
                 progress_fh=None,
                 **kw):
        super().__init__(*args, **kw)
        if not every_rows and not every_pct:
            raise ValueError(
                "You need to specify either 'every_rows' or "
                "'every_pct' to tell how often to callback progress")
        self._object_name = object_name
        self._every_rows = every_rows
        self._every_pct = every_pct
        self._file_len = file_len
        self._callback = callback
        self._progress_fh = progress_fh
        self._last_indication = 0

    def set_current_name(self, name):
        self._object_name = name

    def _check_progress(self):
        do_progress = False
        rows_est = self._file_len / (self.char_num / max(1, self.line_num))
        if self._every_rows:
            do_progress = (
                    self.line_num >= self._last_indication + self._every_rows or
                    self.char_num >= self._file_len - 1)
            self._last_indication = self.line_num
        elif self._every_pct:
            do_progress = (
                    ((pct := int(
                        self.char_num / self._file_len * 100))
                     > self._last_indication)
                    or self.char_num >= self._file_len)
            self._last_indication = pct
        if do_progress:
            if self._callback:
                self._callback(self._object_name, self.line_num,
                               self.char_num, self._file_len)
            elif self._progress_fh:
                self._progress_fh.write(
                    f"\r{self._object_name}: "
                    f"row {self.line_num:,}"
                    f" of {rows_est:,}, "
                    f"{int(self.char_num / self._file_len * 100)}")

    def read(self, *args, **kw):
        data = super().read(*args, **kw)
        self._check_progress()
        return data

    def readline(self, limit=None):
        data = super().readline(limit)
        self._check_progress()
        return data

    def readlines(self, hint=-1) -> List[str]:
        data = super().readlines(hint)
        self._check_progress()
        return data
