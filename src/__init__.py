"""src package marker.

Intentionally empty. The real config + logger live in `src/config.py`
(imported by `src/config_handler.py`).

Earlier revisions duplicated `CustConf`, the logging `defaults` dict, and
created `./working_dir` / `./logs` as import-time side effects. None of
those names were imported anywhere — the package marker is the only thing
that mattered here. Keep it that way: side-effects belong in entry points,
not `__init__.py`.
"""
