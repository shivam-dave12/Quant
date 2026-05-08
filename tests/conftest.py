from pathlib import Path


_read_text = Path.read_text


def _read_text_utf8_default(self, encoding=None, errors=None, newline=None):
    if encoding is None:
        encoding = "utf-8"
    return _read_text(self, encoding=encoding, errors=errors, newline=newline)


Path.read_text = _read_text_utf8_default
