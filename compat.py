"""Compatibility shim for Python < 3.10 (slots=True not supported).

Import this BEFORE any hsb modules to patch dataclass if needed.
"""
import sys
import dataclasses

if sys.version_info < (3, 10):
    _orig = dataclasses.dataclass

    def _compat_dataclass(cls=None, /, **kwargs):
        kwargs.pop('slots', None)  # Remove slots kwarg for Python < 3.10
        if cls is None:
            return lambda c: _orig(c, **kwargs)
        return _orig(cls, **kwargs)

    dataclasses.dataclass = _compat_dataclass
