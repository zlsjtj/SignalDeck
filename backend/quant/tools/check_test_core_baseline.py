#!/usr/bin/env python3
"""
Validate the acceptance baseline for tests/test_core.py.

This script is meant to run in CI before executing the core test module:
- Ensure the expected core test cases are present.
- Fail fast when a test case is renamed/removed/added without baseline update.
"""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from typing import Iterable, List


ROOT = Path(__file__).resolve().parent.parent
BASELINE_PATH = ROOT / "tests" / "baselines" / "test_core_cases.txt"
TEST_MODULE_PATH = ROOT / "tests" / "test_core.py"


def _iter_test_cases(suite: unittest.TestSuite) -> Iterable[unittest.TestCase]:
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from _iter_test_cases(item)
        else:
            yield item


def _load_expected_cases(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"baseline file not found: {path}")
    lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        lines.append(text)
    if not lines:
        raise ValueError(f"baseline file is empty: {path}")
    return sorted(lines)


def _load_actual_cases(module_path: Path) -> List[str]:
    if not module_path.exists():
        raise FileNotFoundError(f"test module not found: {module_path}")

    root_path = str(ROOT)
    if root_path not in sys.path:
        sys.path.insert(0, root_path)

    spec = importlib.util.spec_from_file_location("test_core_baseline_module", str(module_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to build module spec: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    suite = unittest.defaultTestLoader.loadTestsFromModule(module)
    cases = []
    for case in _iter_test_cases(suite):
        name = f"{case.__class__.__name__}.{case._testMethodName}"
        cases.append(name)
    if not cases:
        raise RuntimeError("no tests discovered from tests/test_core.py")
    return sorted(cases)


def main() -> int:
    expected = _load_expected_cases(BASELINE_PATH)
    actual = _load_actual_cases(TEST_MODULE_PATH)

    expected_set = set(expected)
    actual_set = set(actual)
    missing = sorted(expected_set - actual_set)
    unexpected = sorted(actual_set - expected_set)

    if missing or unexpected:
        print("Core test baseline mismatch.")
        if missing:
            print("Missing cases:")
            for item in missing:
                print(f"  - {item}")
        if unexpected:
            print("Unexpected cases:")
            for item in unexpected:
                print(f"  - {item}")
        print(f"Baseline file: {BASELINE_PATH}")
        print("If change is intentional, update the baseline file in the same PR.")
        return 1

    print(f"Core test baseline validated: {len(actual)} cases.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
