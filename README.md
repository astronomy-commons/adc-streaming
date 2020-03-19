# Astronomy Data Commons Genesis Client Libraries

Libraries making it easy to access astronomy data commons resources.

## Developer notes

### Setup

To prepare for development, run `pip install --editable ".[dev]"` from within
the repo directory. This will install all dependencies, including those using
during development workflows.

This project expects you to use a `pip`-centric workflow for development on the
project itself. It gets released to conda, but we don't attempt to record
development-time dependencies like linters through conda tools.

### Code Workflow

Write code, making changes.

Use `make format` to reformat your code to comply with
[black](https://github.com/psf/black).

Use `make lint` to catch common mistakes.

Use `make test` to run tests.

Once satisfied with all three of those, push your changes and open a PR.

### Tag, build, and upload to PyPI and Conda

Tag a new version:
```
git tag -s -a v0.x.x
```

Build and release:

```
make pypi-dist
make pypi-dist-check
make pypi-upload
make conda-build
make conda-upload
```
