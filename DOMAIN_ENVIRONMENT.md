# Reproducible offline environment

Upstream: https://github.com/singer-io/singer-python
Upstream SHA: f9c3c1870d13eb32684896f20373846606b33a31
Runtime: 3.13.14 (tags/v3.13.14:fd17997, Jun 10 2026, 13:03:48) [MSC v.1944 64 bit (AMD64)]
Platform: win32

Create an isolated virtual environment, then run from this checkout:
```
python -m pip install -r requirements-domain.lock
python -m pip install -e .
```
Set `PYTHONUTF8=1` and `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1` before tests.
```
python -m pytest -q -o addopts= -o log_cli=false tests/test_catalog.py tests/test_state.py tests/test_schema.py tests/test_singer.py tests/test_transform.py tests/test_metadata.py
```
Measured selected pytest process: 1.207 seconds.
Only the listed core tests are claimed. Optional integrations are not required.

Tests use synthetic in-memory or temporary-file inputs under the upstream license.
No account, cloud service, browser or external database is needed. Recreate the
environment from this immutable commit to reset the starting state.
