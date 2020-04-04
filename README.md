# Astronomy Data Commons Streaming Client Libraries

Libraries making it easy to access astronomy data commons resources.

## Developer notes

### Setup

To prepare for development, run `pip install --editable ".[dev]"` from within
the repo directory. This will install all dependencies, including those using
during development workflows.

This project expects you to use a `pip`-centric workflow for development on the
project itself. If you're using conda, then use the conda environment's `pip` to
install development dependencies, as described above.

Integration tests require Docker to run a Kafka broker. The broker might have
network problems on OSX if you use Docker Desktop; run the tests in a Linux
virtual machine (like with VirtualBox) to get around this.

### Code Workflow

Write code, making changes.

Use `make format` to reformat your code to comply with PEP8.

Use `make lint` to catch common mistakes.

Use `make test-quick` to run fast unit tests.

Use `make test` to run the full slow test suite, including integration tests.

Once satisfied with all four of those, push your changes and open a PR.

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
