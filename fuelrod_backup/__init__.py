"""fuelrod-backup: Interactive multi-engine database backup and restore CLI."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("fuelrod-backup")
except PackageNotFoundError:
    __version__ = "0.1.0"
