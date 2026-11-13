"""Minimal pytest shim for environments without pytest (supports the subset used here).

Provides tmp_path fixtures, monkeypatch and pytest.raises context manager.
Not a replacement for pytest in development; the project's declared test
dependency remains pytest 8.3.x.
"""

import contextlib
import importlib.util
import inspect
import pathlib
import sys
import tempfile
import traceback


class MonkeyPatch:
    def __init__(self):
        self._changes = []

    def setattr(self, target, name, value):
        old = getattr(target, name)
        self._changes.append((target, name, old))
        setattr(target, name, value)

    def undo(self):
        for target, name, old in reversed(self._changes):
            setattr(target, name, old)
        self._changes.clear()


class RaisesProxy:
    def __init__(self, expected):
        self.expected = expected
        self.match = None
        self.value = None

    def __call__(self, expected, match=None):
        self.expected = expected
        self.match = match
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            raise AssertionError(f"{self.expected.__name__} was not raised")
        if not issubclass(exc_type, self.expected):
            return False
        if self.match is not None and self.match not in str(exc):
            raise AssertionError(f"pattern {self.match!r} not found in {exc!s}")
        self.value = exc
        return True


raises_singleton = RaisesProxy(RuntimeError)


def raises(expected, match=None):
    return RaisesProxy(expected)(expected, match)


def run_module(path: str) -> int:
    spec = importlib.util.spec_from_file_location("test_module_under_test", path)
    module = importlib.util.module_from_spec(spec)
    module.pytest = sys.modules[__name__]
    spec.loader.exec_module(module)

    tests = [
        (name, obj)
        for name, obj in vars(module).items()
        if name.startswith("test_") and callable(obj)
    ]
    failures = 0
    for name, fn in tests:
        params = inspect.signature(fn).parameters
        kwargs = {}
        undo_stack = []
        if "tmp_path" in params:
            tmp = tempfile.mkdtemp(prefix=f"{name}_")
            kwargs["tmp_path"] = pathlib.Path(tmp)
        if "monkeypatch" in params:
            patch = MonkeyPatch()
            kwargs["monkeypatch"] = patch
            undo_stack.append(patch)
        try:
            fn(**kwargs)
            print(f"PASS {name}")
        except Exception:
            failures += 1
            print(f"FAIL {name}")
            traceback.print_exc()
        finally:
            for patch in undo_stack:
                patch.undo()
    print(f"\n{len(tests) - failures}/{len(tests)} passed")
    return 1 if failures else 0
