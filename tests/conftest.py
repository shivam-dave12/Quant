import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

os.environ.setdefault("GROWW_ACCESS_TOKEN", "test-groww-token")
os.environ.setdefault("GROWW_ENABLED", "true")
os.environ.setdefault("GROWW_DISCOVERY_ENABLED", "true")

_orig_read_text = Path.read_text


def _utf8_read_text(self, encoding=None, errors=None, newline=None):
    return _orig_read_text(self, encoding=encoding or "utf-8", errors=errors, newline=newline)


Path.read_text = _utf8_read_text
