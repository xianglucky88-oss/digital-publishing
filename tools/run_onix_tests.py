"""Entry point: run tests/test_onix.py against the bundled mini_pytest shim."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
sys.path.insert(0, str(ROOT / "src"))

import mini_pytest  # noqa: E402

sys.modules["pytest"] = mini_pytest
sys.exit(mini_pytest.run_module(str(ROOT / "tests" / "test_onix.py")))
