try:
    from importlib.metadata import PackageNotFoundError, version
except ImportError:
    # NOTE: remove after dropping support for Python < 3.8
    from importlib_metadata import PackageNotFoundError, version

try:
    __version__ = version("adc-streaming")
except PackageNotFoundError:
    # package is not installed
    pass
