try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:
    # NOTE: remove after dropping support for Python < 3.8
    from importlib_metadata import version, PackageNotFoundError

try:
    __version__ = version("adc-streaming")
except PackageNotFoundError:
    # package is not installed
    pass
