try:
    from ._version import version as __version__
except ImportError:
    # package is not installed
    __version__ = "dev"
