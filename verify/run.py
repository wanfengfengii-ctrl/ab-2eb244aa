"""One-shot verification service.

Runs (in order):
  1. wait for gateway + simulator health checks,
  2. build check: byte-compile every Python source,
  3. unit tests (reader, store, validation),
  4. end-to-end smoke: stable read, revision-change retry, idempotent replay,
     conflict and transport-anomaly handling.

Exits with a bitmask (0 = all good):
  bit 0 (1)  build check failed
  bit 1 (2)  unit tests failed
  bit 2 (4)  end-to-end smoke failed
  bit 3 (8)  services never became healthy
"""

import os
import py_compile
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from tests.smoke_suite import CheckRunner, wait_healthy  # noqa: E402

GATEWAY = os.environ.get("GATEWAY_URL", "http://gateway:8080")
SIMULATOR = os.environ.get("SIMULATOR_URL", "http://simulator:8080")

EXIT_BUILD = 1
EXIT_UNIT = 2
EXIT_SMOKE = 4
EXIT_HEALTH = 8

SOURCE_DIRS = ["common", "gateway", "simulator", "tests", "verify"]


def collect_sources():
    sources = []
    for directory in SOURCE_DIRS:
        base = os.path.join(ROOT, directory)
        for name in sorted(os.listdir(base)):
            if name.endswith(".py"):
                sources.append(os.path.join(base, name))
    return sources


def build_check():
    print("== build check (byte-compile) ==", flush=True)
    failed = []
    for source in collect_sources():
        try:
            py_compile.compile(source, doraise=True)
            print(f"PASS  compile {os.path.relpath(source, ROOT)}")
        except py_compile.PyCompileError as exc:
            failed.append(source)
            print(f"FAIL  compile {source}: {exc}")
    return not failed


def unit_tests():
    print("== unit tests ==", flush=True)
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromName("tests.test_unit")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return result.wasSuccessful()


def smoke_tests():
    print("== end-to-end smoke ==", flush=True)
    runner = CheckRunner(GATEWAY, SIMULATOR)
    return runner.run()


def main():
    code = 0
    try:
        wait_healthy(f"{GATEWAY}/health")
        wait_healthy(f"{SIMULATOR}/health")
        print("PASS  gateway and simulator healthy", flush=True)
    except RuntimeError as exc:
        print(f"FAIL  health: {exc}", flush=True)
        return EXIT_HEALTH

    if not build_check():
        code |= EXIT_BUILD
    if not unit_tests():
        code |= EXIT_UNIT
    if not smoke_tests():
        code |= EXIT_SMOKE

    if code == 0:
        print("ALL CHECKS PASSED", flush=True)
    else:
        print(f"VERIFICATION FAILED (exit code {code})", flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
