import io
from typing import List


class TextIOStatsWrapper(io.TextIOWrapper):  # (RawIOWrapper):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self._char_num = 0
        self._line_num = -1

    def read(self, *args, **kw):
        data = super().read(*args, **kw)
        self._char_num += len(data)
        return data  #.decode('utf-8')

    def readline(self, limit=None):
        data = super().readline(limit if limit is not None else -1)
        self._char_num += len(data)
        self._line_num += 1
        return data  #.decode('utf-8')

    def readlines(self, hint=-1) -> List[str]:
        data = super().readlines(hint)
        # for i, d in enumerate(data):
        self._char_num += sum(len(d) for d in data)
        self._line_num += len(data)
        # data[i] = d  #.decode('utf-8')
        return data

    @property
    def char_num(self):
        return self._char_num

    @property
    def line_num(self):
        return self._line_num
