

<p align="center">
  <img src="https://depeche-py.github.io/depeche-db/assets/logo-bg.png" width="200" />
</p>

# Depeche DB

A library for building event-based systems on top of PostgreSQL

[![Tests](https://github.com/depeche-py/depeche-db/actions/workflows/tests.yml/badge.svg)](https://github.com/depeche-py/depeche-db/actions/workflows/tests.yml)
[![pypi](https://img.shields.io/pypi/v/depeche-db.svg)](https://pypi.python.org/pypi/depeche-db)
[![versions](https://img.shields.io/pypi/pyversions/depeche-db.svg)](https://github.com/depeche-py/depeche-db)
[![Docs](https://img.shields.io/badge/docs-here-green.svg)](https://depeche-py.github.io/depeche-db/)
[![license](https://img.shields.io/github/license/depeche-py/depeche-db.svg)](https://github.com/depeche-py/depeche-db/blob/main/LICENSE)

---

**Documentation**: [https://depeche-py.github.io/depeche-db/](https://depeche-py.github.io/depeche-db/)

**Source code**: [https://github.com/depeche-py/depeche-db](https://github.com/depeche-py/depeche-db)

---

Depeche DB is modern Python library for building event-based systems

Key features:

* Message store with optimistic concurrency control & strong ordering guarantees
* Subscriptions with "at least once" semantics
* Parallel processing of (partitioned) subscriptions
* No database polling

## Requirements

Python 3.9+
SQLAlchemy 1.4 or 2+
PostgreSQL 12+
Psycopg (Version 2 >= 2.9.3 or Version 3 >= 3.2)


## Installation

```bash
pip install depeche-db
# OR
poetry add depeche-db
```

## Example

```python
%EXAMPLE%
```


## Contribute

Contributions in the form of issues, questions, feedback and pull requests are
welcome. Before investing a lot of time, let me know what you are up to so
we can see if your contribution fits the vision of the project.
