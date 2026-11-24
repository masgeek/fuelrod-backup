"""The registry of every configuration key, so that `init` can offer all of them.

Each key the config loader reads has one `Setting` here: what it is, its type, default, the group it
belongs to and (for keys with a dedicated wizard) which command sets it. `init --list` prints the
registry, `init --section <name>` walks one group, and a test fails when a key is read in
``config.py`` without an entry here, so a new option can never be left out of the wizards.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

# Groups the advanced wizard walks, in order: name -> title
SECTIONS: dict[str, str] = {
    "storage": "Backup storage and retention",
    "backup": "Backup behaviour (incremental, hooks, size warning)",
    "encryption": "Encryption at rest",
    "notifications": "Notifications",
    "push": "Push (sync) behaviour",
    "quota": "Remote quota",
    "pull": "Pull behaviour",
    "rclone": "rclone",
    "n8n": "n8n volume backup",
}


@dataclass(frozen=True)
class Setting:
    key: str
    group: str  # a SECTIONS name, or "core" / "remote" for keys other wizards own
    kind: str  # text | bool | int | float | choice | secret | path | size | list
    default: str
    help: str
    choices: tuple[str, ...] = ()
    minimum: int | None = None
    maximum: int | None = None
    depends_on: tuple[str, ...] = ()  # (key, "true") / (key, "*") = only ask when that key is set
    wizard: str = "init"  # the command that asks for it
    validate: Callable[[str], None] | None = field(default=None, compare=False)

    @property
    def advanced(self) -> bool:
        return self.group in SECTIONS


def _check_retention(value: str) -> None:
    from .retention import parse_profile

    parse_profile(value)


def _check_size(value: str) -> None:
    from .quota import parse_size

    parse_size(value)


def _check_url(value: str) -> None:
    if not value.lower().startswith(("http://", "https://")):
        raise ValueError("must start with http:// or https://")


def _check_ratio(value: str) -> None:
    if float(value) < 0:
        raise ValueError("must be 0 or more")


S = Setting
SETTINGS: tuple[Setting, ...] = (
    # ── connection + core storage (asked by the main `init` wizard) ──
    S("DB_TYPE", "core", "choice", "postgres", "Database engine", ("postgres", "mariadb", "mssql")),
    S("DB_USERNAME", "core", "text", "", "Database user"),
    S("DB_PASSWORD", "core", "secret", "", "Database password"),
    S("DB_HOST", "core", "text", "127.0.0.1", "Database host"),
    S("DB_PORT", "core", "int", "", "Database port", minimum=1, maximum=65535),
    S("USE_DOCKER", "core", "bool", "true", "Run the dump/restore tools inside a Docker container"),
    S("SERVICE", "core", "text", "", "Docker container name"),
    S("BASE_DIR", "core", "path", "", "Backup root directory"),
    S("COMPRESS_FILE", "core", "bool", "false", "Compress backups with gzip"),
    S("KEEP_DAYS", "core", "int", "7", "Keep backups for N days (0 = forever)", minimum=0),
    S("CONNECTION_TIMEOUT", "core", "int", "30", "Connection timeout in seconds", minimum=1),
    S("PG_DUMP_CMD", "core", "text", "pg_dump", "pg_dump command"),
    S("PG_RESTORE_CMD", "core", "text", "pg_restore", "pg_restore command"),
    S("PSQL_CMD", "core", "text", "psql", "psql command"),
    S("MYSQL_DUMP_CMD", "core", "text", "mysqldump", "mysqldump / mariadb-dump command"),
    S("MYSQL_CMD", "core", "text", "mysql", "mysql client command"),
    S("MSSQL_BACKUP_DIR", "core", "text", "/var/opt/mssql/backups", "SQL Server backup directory inside the container"),

    # ── storage ──
    S("RETENTION", "storage", "text", "",
      "GFS retention, e.g. daily=7,weekly=4,monthly=12 (overrides KEEP_DAYS; blank = use KEEP_DAYS)",
      validate=_check_retention),
    S("MANIFEST", "storage", "bool", "true", "Keep manifest.json, an index of all backups that pull reads"),

    # ── backup behaviour ──
    S("INCREMENTAL", "backup", "bool", "false", "Incremental backups (PostgreSQL: dump only changed tables)"),
    S("INCREMENTAL_KEEP", "backup", "int", "5", "Incremental dumps to keep per base", minimum=1,
      depends_on=("INCREMENTAL", "true")),
    S("SIZE_WARN_RATIO", "backup", "float", "0.5",
      "Warn when a dump is smaller than this fraction of the previous one (0 = off)", validate=_check_ratio),
    S("PRE_BACKUP_HOOK", "backup", "text", "", "Command run before a backup; a non-zero exit aborts it"),
    S("POST_BACKUP_HOOK", "backup", "text", "", "Command run after a backup; failure only warns"),

    # ── encryption ──
    S("ENCRYPT", "encryption", "bool", "false", "Encrypt backups at rest"),
    S("ENCRYPT_TOOL", "encryption", "choice", "age", "Encryption tool", ("age", "gpg"), depends_on=("ENCRYPT", "true")),
    S("ENCRYPT_RECIPIENT", "encryption", "text", "", "Recipient (age public key or gpg key id)",
      depends_on=("ENCRYPT", "true")),
    S("ENCRYPT_IDENTITY_FILE", "encryption", "path", "", "Private key file used to decrypt (age identity)",
      depends_on=("ENCRYPT", "true")),
    S("ENCRYPT_PASSPHRASE_FILE", "encryption", "path", "", "File holding a passphrase (gpg symmetric)",
      depends_on=("ENCRYPT", "true")),

    # ── notifications ──
    S("NOTIFY_ON", "notifications", "choice", "", "When to notify", ("", "success", "failure", "all")),
    S("NOTIFY_URL", "notifications", "text", "", "Webhook URL (JSON POST)", depends_on=("NOTIFY_ON", "*"),
      validate=_check_url),
    S("NOTIFY_TIMEOUT", "notifications", "int", "15", "Notification timeout in seconds", minimum=1,
      depends_on=("NOTIFY_ON", "*")),
    S("NOTIFY_HOOK_SUCCESS", "notifications", "text", "", "Command run on success ({status} {command} {summary})",
      depends_on=("NOTIFY_ON", "*")),
    S("NOTIFY_HOOK_FAILURE", "notifications", "text", "", "Command run on failure", depends_on=("NOTIFY_ON", "*")),

    # ── push ──
    S("PUSH_REMOTE", "remote", "text", "", "Push destinations (rclone remotes / folders, | separated)",
      wizard="push-init"),
    S("PUSH_LATEST", "push", "int", "3", "Newest backups per database a push uploads (0 = all)", minimum=0),
    S("PUSH_DELETE_LOCAL", "push", "bool", "true", "Delete local files after a successful push"),

    # ── quota ──
    S("PUSH_QUOTA", "quota", "size", "", "Remote size limit, e.g. 50G (blank = no quota)", validate=_check_size),
    S("PUSH_QUOTA_TARGET", "quota", "int", "90", "Prune down to this percent of the quota", minimum=1, maximum=100,
      depends_on=("PUSH_QUOTA", "*")),
    S("PUSH_QUOTA_KEEP", "quota", "int", "1", "Newest backups per database never pruned", minimum=0,
      depends_on=("PUSH_QUOTA", "*")),

    # ── pull ──
    S("PULL_REMOTE", "remote", "text", "", "rclone remote pull downloads from", wizard="pull-init"),
    S("PULL_EXCLUDE", "pull", "list", "", "Globs of files pull never downloads, | separated"),
    S("PULL_DELETE_REMOTE", "pull", "bool", "false", "Delete server files after they are verified locally"),
    S("PULL_KEEP_REMOTE", "pull", "int", "0", "Newest backups per database left on the server", minimum=0,
      depends_on=("PULL_DELETE_REMOTE", "true")),

    # ── rclone ──
    S("RCLONE_CMD", "rclone", "text", "rclone", "rclone executable (name on PATH or full path)"),
    S("REMOTE", "remote", "text", "", "Generic rclone remote for sync", wizard="sync-init"),
    S("S3_REMOTE", "remote", "text", "", "rclone remote name for S3", wizard="sync-init"),
    S("S3_BUCKET", "remote", "text", "", "S3 bucket", wizard="sync-init"),
    S("S3_PREFIX", "remote", "text", "", "S3 key prefix", wizard="sync-init"),
    S("S3_PROVIDER", "remote", "text", "", "S3 provider", wizard="sync-init"),
    S("S3_REGION", "remote", "text", "", "S3 region", wizard="sync-init"),
    S("S3_ENDPOINT", "remote", "text", "", "S3 endpoint", wizard="sync-init"),
    S("S3_ACCESS_KEY_ID", "remote", "text", "", "S3 access key id", wizard="sync-init"),
    S("S3_SECRET_ACCESS_KEY", "remote", "secret", "", "S3 secret access key", wizard="sync-init"),

    # ── n8n ──
    S("N8N_SERVICES", "n8n", "list", "n8n", "Docker services to snapshot with n8n-backup (space separated)"),
    S("SKIP_SERVICES", "n8n", "list", "", "Services n8n-backup must skip (space separated)"),
)

BY_KEY: dict[str, Setting] = {s.key: s for s in SETTINGS}


def in_section(name: str) -> list[Setting]:
    return [s for s in SETTINGS if s.group == name]


def _is_set(value: str | None, expected: str) -> bool:
    if expected == "*":
        return bool((value or "").strip())
    return (value or "").strip().lower() in ("true", "1", "yes", "on") if expected == "true" else value == expected


def applies(setting: Setting, values: dict[str, str]) -> bool:
    """False when the setting only matters if another one is switched on and it is not."""
    if not setting.depends_on:
        return True
    dep_key, expected = setting.depends_on
    dep = BY_KEY.get(dep_key)
    current = values.get(dep_key, dep.default if dep else "")
    return _is_set(current, expected)


def normalise(setting: Setting, raw: str) -> str:
    """Validate *raw* for *setting* and return the text to store. Raises ValueError with a readable message."""
    value = raw.strip()
    if setting.kind == "bool":
        if value.lower() not in ("true", "false", "1", "0", "yes", "no", "on", "off"):
            raise ValueError("answer true or false")
        return "true" if value.lower() in ("true", "1", "yes", "on") else "false"
    if setting.kind in ("int", "float"):
        if not value:
            return ""
        try:
            number = int(value) if setting.kind == "int" else float(value)
        except ValueError:
            raise ValueError(f"{value!r} is not a number") from None
        if setting.minimum is not None and number < setting.minimum:
            raise ValueError(f"must be at least {setting.minimum}")
        if setting.maximum is not None and number > setting.maximum:
            raise ValueError(f"must be at most {setting.maximum}")
    if setting.kind == "choice" and value not in setting.choices:
        raise ValueError("choose one of: " + ", ".join(c or "(none)" for c in setting.choices))
    if value and setting.validate is not None:
        setting.validate(value)
    return value
