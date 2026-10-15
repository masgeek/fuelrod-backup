"""Encryption at rest for backup artifacts.

Supported tools: `age` (default) and `gpg`. Encryption is applied after
compression and produces `<artifact>.age` / `<artifact>.gpg`; the plaintext is
removed. Restore detects the suffix and decrypts to a temporary file that is
cleaned up when the process exits.

Key material is never passed on the command line — age uses a recipient public
key and an identity file, gpg uses a recipient key or a passphrase file read
from disk. Every subprocess gets an explicit minimal environment.
"""

from __future__ import annotations

import atexit
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from .config import Config

ENCRYPTED_SUFFIXES = (".age", ".gpg")

# Temporary plaintext files created while restoring; wiped at exit.
_TEMP_FILES: list[Path] = []


class CryptoError(RuntimeError):
    """Raised when encryption or decryption cannot be performed."""


def _cleanup_temp_files() -> None:
    for path in _TEMP_FILES:
        try:
            if path.exists():
                path.unlink()
        except OSError:
            pass
    _TEMP_FILES.clear()


atexit.register(_cleanup_temp_files)


# Variables the crypto tools need to find their own state (keyring, config
# dir). Everything else is dropped so database credentials cannot leak into a
# subprocess.
_ALLOWED_ENV = (
    "PATH", "HOME", "USERPROFILE", "APPDATA", "LOCALAPPDATA",
    "GNUPGHOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME",
    "SystemRoot", "TEMP", "TMP", "LANG",
)


def _tool_env(passphrase_file: str | None = None) -> dict[str, str]:
    """
    Minimal environment for age/gpg — the parent environment is never inherited.

    Only the variables the tools need for their own state are passed through;
    the parent environment would otherwise leak database passwords into the
    subprocess. Missing keys are simply absent, not empty.
    """
    env = {key: os.environ[key] for key in _ALLOWED_ENV if key in os.environ}
    env["LC_ALL"] = "C"
    return env


def tool_available(tool: str) -> bool:
    return shutil.which(tool) is not None


def is_encrypted(path: Path | str) -> bool:
    return str(path).endswith(ENCRYPTED_SUFFIXES)


def validate_config(cfg: Config) -> None:
    """
    Raise CryptoError if encryption is requested but not usable.

    Called before a backup starts, so a misconfiguration fails immediately
    rather than after dumping the whole database.
    """
    if not cfg.encrypt:
        return
    if not tool_available(cfg.encrypt_tool):
        raise CryptoError(
            f"{cfg.encrypt_tool} not found in PATH. Install it, set ENCRYPT_TOOL, "
            "or set ENCRYPT=false."
        )
    if cfg.encrypt_tool == "age":
        if not cfg.encrypt_recipient:
            raise CryptoError("ENCRYPT_RECIPIENT is required (an age public key, e.g. age1...).")
    else:
        if not cfg.encrypt_recipient and not cfg.encrypt_passphrase_file:
            raise CryptoError(
                "Set ENCRYPT_RECIPIENT (gpg key) or ENCRYPT_PASSPHRASE_FILE (symmetric)."
            )
        if cfg.encrypt_passphrase_file and not Path(cfg.encrypt_passphrase_file).is_file():
            raise CryptoError(f"Passphrase file not found: {cfg.encrypt_passphrase_file}")


def _destination(src: Path, tool: str) -> Path:
    return Path(str(src) + (".age" if tool == "age" else ".gpg"))


def encrypt_file(src: Path, cfg: Config, *, remove_plaintext: bool = True) -> Path:
    """
    Encrypt *src* and return the new path.

    With ``remove_plaintext=True`` the plaintext is deleted only after the
    ciphertext is confirmed to exist, so a failed encryption cannot destroy the
    only copy of a backup.
    """
    validate_config(cfg)
    dest = _destination(src, cfg.encrypt_tool)
    passphrase_file = cfg.encrypt_passphrase_file or None

    if cfg.encrypt_tool == "age":
        cmd = [cfg.encrypt_tool, "-r", cfg.encrypt_recipient, "-o", str(dest), str(src)]
    elif cfg.encrypt_recipient:
        cmd = [
            cfg.encrypt_tool, "--batch", "--yes",
            "--encrypt", "--recipient", cfg.encrypt_recipient,
            "-o", str(dest), str(src),
        ]
    else:
        cmd = [
            cfg.encrypt_tool, "--batch", "--yes",
            "--passphrase-file", str(passphrase_file),
            "--symmetric", "-o", str(dest), str(src),
        ]

    try:
        subprocess.run(cmd, check=True, env=_tool_env(passphrase_file))  # noqa: S603
    except subprocess.CalledProcessError as exc:
        raise CryptoError(f"Encryption failed (exit {exc.returncode}) for {src.name}") from exc
    except FileNotFoundError as exc:
        raise CryptoError(f"{cfg.encrypt_tool} not found in PATH") from exc

    if not dest.is_file() or dest.stat().st_size == 0:
        raise CryptoError(f"Encryption produced no output for {src.name}")

    if remove_plaintext:
        try:
            src.unlink()
        except OSError as exc:
            raise CryptoError(
                f"Encrypted to {dest.name} but could not remove plaintext {src.name}: {exc}"
            ) from exc

    return dest


def decrypt_file(src: Path, cfg: Config) -> Path:
    """
    Decrypt *src* into a temporary file and return its path.

    The plaintext lives in the system temp directory and is removed when the
    process exits (see _cleanup_temp_files).
    """
    tool = "age" if src.name.endswith(".age") else "gpg"
    if not tool_available(tool):
        raise CryptoError(
            f"{tool} not found in PATH — cannot decrypt {src.name}. Install {tool} "
            "or restore the plaintext backup."
        )

    inner = src.name
    for suffix in ENCRYPTED_SUFFIXES:
        inner = inner[: -len(suffix)] if inner.endswith(suffix) else inner

    fd, name = tempfile.mkstemp(prefix="fuelrod-decrypt-", suffix=Path(inner).suffix or ".dump")
    os.close(fd)
    dest = Path(name)

    if tool == "age":
        cmd = [tool, "-d", "-o", str(dest)]
        if cfg.encrypt_identity_file:
            cmd += ["-i", cfg.encrypt_identity_file]
        cmd.append(str(src))
    else:
        cmd = [tool, "--batch", "--yes", "--output", str(dest)]
        if cfg.encrypt_passphrase_file:
            cmd += ["--passphrase-file", str(cfg.encrypt_passphrase_file)]
        cmd += ["--decrypt", str(src)]

    try:
        subprocess.run(cmd, check=True, env=_tool_env(cfg.encrypt_passphrase_file or None))  # noqa: S603
    except subprocess.CalledProcessError as exc:
        dest.unlink(missing_ok=True)
        raise CryptoError(
            f"Decryption failed (exit {exc.returncode}) for {src.name} — "
            "check ENCRYPT_IDENTITY_FILE / the passphrase."
        ) from exc
    except FileNotFoundError as exc:
        dest.unlink(missing_ok=True)
        raise CryptoError(f"{tool} not found in PATH") from exc

    if dest.stat().st_size == 0:
        dest.unlink(missing_ok=True)
        raise CryptoError(f"Decryption produced an empty file for {src.name}")

    _TEMP_FILES.append(dest)
    return dest


def decrypt_if_needed(path: Path, cfg: Config) -> Path:
    """Return a decrypted working copy when *path* is encrypted, else *path*."""
    if not is_encrypted(path):
        return path
    return decrypt_file(path, cfg)
