# MCAP Python Library

The `mcap` library provides classes for reading and writing the MCAP file format.

## Semantic Versioning Notes

Each python package in this repository contains a `__version__` string attribute in the root module:

```python
from mcap import __version__
print(__version__)
```

Modules, variables, classes, attributes etc. with names preceded by a single `_` underscore are not considered part of the public API and may change between minor or patch releases.
