from io import IOBase, BufferedIOBase
from typing import List


class BytesIOStatsWrapper(IOBase):

    def __init__(self, source: BufferedIOBase):
        self._source = source
        self._char_num = 0
        self._line_num = -1

    def read(self, *args, **kw):
        data = self._source.read(*args, **kw)
        self._char_num += len(data)
        self._line_num += data.count(b"\n")
        return data

    def readline(self, limit=None):
        data = self._source.readline(limit if limit is not None else -1)
        self._char_num += len(data)
        self._line_num += 1
        return data

    def readlines(self, hint=-1) -> List[bytes]:
        data = self._source.readlines(hint)
        self._char_num += sum(len(d) for d in data)
        self._line_num += len(data)
        return data

    @property
    def char_num(self):
        return self._char_num

    @property
    def line_num(self):
        return self._line_num

    def close(self):
        """
        Flush and close the IO object.

        This method has no effect if the file is already closed.
        """
        return self._source.close()

    def fileno(self):
        """
        Returns underlying file descriptor if one exists.

        OSError is raised if the IO object does not use a file descriptor.
        """
        return self._source.fileno()

    def flush(self):
        """
        Flush write buffers, if applicable.

        This is not implemented for read-only and non-blocking streams.
        """
        return self._source.flush()

    def isatty(self):
        """
        Return whether this is an 'interactive' stream.

        Return False if it can't be determined.
        """
        return self._source.isatty()

    def readable(self):
        """
        Return whether object was opened for reading.

        If False, read() will raise OSError.
        """
        return self._source.readable()

    def seek(self, *args, **kwargs):
        """
        Change stream position.

        Change the stream position to the given byte offset. The offset is
        interpreted relative to the position indicated by whence.  Values
        for whence are:

        * 0 -- start of stream (the default); offset should be zero or positive
        * 1 -- current stream position; offset may be negative
        * 2 -- end of stream; offset is usually negative

        Return the new absolute position.
        """
        return self._source.seek(*args, **kwargs)

    def seekable(self):
        """
        Return whether object supports random access.

        If False, seek(), tell() and truncate() will raise OSError.
        This method may need to do a test seek().
        """
        return self._source.seekable()

    def tell(self):
        """ Return current stream position. """
        return self._source.tell()

    def truncate(self, *args, **kwargs):
        """
        Truncate file to size bytes.

        File pointer is left unchanged.  Size defaults to the current IO
        position as reported by tell().  Returns the new size.
        """
        return self._source.truncate(*args, **kwargs)

    def writable(self):
        """
        Return whether object was opened for writing.

        If False, write() will raise OSError.
        """
        return self._source.writable()

    def writelines(self, *args, **kwargs):
        """
        Write a list of lines to stream.

        Line separators are not added, so it is usual for each of the
        lines provided to have a line separator at the end.
        """
        return self._source.writelines(*args, **kwargs)

    def _checkClosed(self, *args, **kwargs):
        return self._source._checkClosed(*args, **kwargs)

    def __del__(self, *args, **kwargs):
        return self._source.__del__()

    def __enter__(self, *args, **kwargs):
        return self._source.__enter__()

    def __exit__(self, *args, **kwargs):
        return self._source.__exit__(*args, **kwargs)

    def __iter__(self, *args, **kwargs):
        """ Implement iter(self). """
        return self._source.__iter__()

    @staticmethod
    def __new__(*args, **kwargs):
        """ Create and return a new object.  See help(type) for accurate signature. """
        return IOBase.__new__(*args, **kwargs)

    def __next__(self):  # real signature unknown
        """ Implement next(self). """
        return self._source.__next__()

    closed = property(lambda self: object(), lambda self, v: None,
                      lambda self: None)  # default

    __dict__ = None
