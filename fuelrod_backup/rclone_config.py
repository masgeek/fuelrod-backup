"""Read and write rclone remote configuration.

`init` uses this to create an S3 remote without the operator hand-writing
rclone's INI file. The rest of the app only ever reads: `sync` invokes the
`rclone` binary, which owns the config.

The file is edited in place with `configparser` so unrelated remotes survive.
"""

from __future__ import annotations

import os
import stat
from configparser import ConfigParser
from configparser import Error as ConfigParserError
from pathlib import Path

# rclone keeps its config here unless RCLONE_CONFIG says otherwise.
_WINDOWS_PATH = Path(os.environ.get("APPDATA", "")) / "rclone" / "rclone.conf"
_UNIX_PATH = Path(os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))) / "rclone" / "rclone.conf"


class RcloneConfigError(RuntimeError):
    """Raised when the rclone config cannot be read or written."""


def config_path() -> Path:
    """Path to the active rclone config file."""
    override = os.environ.get("RCLONE_CONFIG")
    if override:
        return Path(override)
    return _WINDOWS_PATH if os.name == "nt" else _UNIX_PATH


def _parser() -> ConfigParser:
    parser = ConfigParser(interpolation=None)
    parser.optionxform = str  # preserve key case the way rclone writes it
    path = config_path()
    if path.is_file():
        try:
            parser.read(path, encoding="utf-8")
        except (ConfigParserError, UnicodeDecodeError, OSError) as exc:
            raise RcloneConfigError(f"Cannot read {path}: {exc}") from exc
    return parser


def list_remotes() -> list[str]:
    """Names of the remotes defined in the config file."""
    return _parser().sections()


def remote_exists(name: str) -> bool:
    return name in list_remotes()


def write_remote(name: str, section_type: str, options: dict[str, str]) -> Path:
    """
    Create or update the remote *name* and return the config path.

    Existing keys are preserved; only the supplied options are changed, so
    re-running init does not wipe unrelated settings in the section.
    """
    if not name or any(c in name for c in " :\t"):
        raise RcloneConfigError(f"Invalid remote name: {name!r}")

    parser = _parser()
    if not parser.has_section(name):
        parser.add_section(name)
    parser.set(name, "type", section_type)
    for key, value in options.items():
        if value:
            parser.set(name, key, value)

    path = config_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            parser.write(fh)
        # The file holds credentials — keep it owner-only where supported.
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    except OSError as exc:
        raise RcloneConfigError(f"Cannot write {path}: {exc}") from exc
    return path


def s3_options(
        access_key_id: str,
        secret_access_key: str,
        provider: str = "",
        region: str = "",
        endpoint: str = "",
        env_auth: bool = False,
) -> dict[str, str]:
    """Build the rclone option set for an S3 remote."""
    options: dict[str, str] = {}
    if env_auth:
        options["env_auth"] = "true"
    else:
        options["access_key_id"] = access_key_id
        options["secret_access_key"] = secret_access_key
    if provider:
        options["provider"] = provider
    if region:
        options["region"] = region
    if endpoint:
        options["endpoint"] = endpoint
    return options


# S3-compatible providers offered by the init wizard, with their rclone value.
S3_PROVIDERS = [
    ("Amazon S3", ""),
    ("MinIO", "Minio"),
    ("Wasabi", "Wasabi"),
    ("DigitalOcean Spaces", "DigitalOcean"),
    ("Backblaze B2", "Backblaze"),
    ("Ceph / other S3-compatible", "Ceph"),
]
