"""Tests for scheduling backup / pull / push jobs."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from fuelrod_backup import schedule as sched


class TestBuildCommand:
    def test_each_job_runs_its_own_command_non_interactively(self):
        cmds = {job: sched.build_command(job, None, None) for job in sched.JOBS}
        assert cmds["backup"][1:3] == ["backup", "--no-interactive"]
        assert cmds["pull"][1] == "pull" and cmds["push"][1:3] == ["push", "--no-interactive"]
        assert cmds["quota"][1] == "quota" and cmds["verify"][1:3] == ["verify", "--latest"]
        assert all("--json" in c for c in cmds.values())

    def test_db_type_only_goes_to_jobs_that_accept_it(self):
        assert "--db-type" in sched.build_command("backup", "mariadb", None)
        assert "--db-type" not in sched.build_command("pull", "mariadb", None)
        assert "--db-type" not in sched.build_command("push", "mariadb", None)

    def test_config_and_extra_arguments(self):
        cmd = sched.build_command("push", None, "C:/cfg/.backup", ["--latest 5", "--keep-local"])
        assert cmd[-5:] == ["--config", "C:/cfg/.backup", "--latest", "5", "--keep-local"]

    def test_quoted_extra_arguments_stay_together(self):
        cmd = sched.build_command("pull", None, None, ["-x '*_inc.*'"])
        assert cmd[-2:] == ["-x", "*_inc.*"]

    def test_unknown_job(self):
        with pytest.raises(ValueError, match="unknown job"):
            sched.build_command("nope", None, None)


class TestRender:
    CMD = ["/usr/bin/fuelrod-backup", "pull", "--json"]

    def test_cron_line_is_tagged_with_the_job(self):
        text, argv = sched.render("pull", "30 3 * * *", self.CMD, "pull.log", windows=False)
        assert argv is None
        assert text == "30 3 * * * /usr/bin/fuelrod-backup pull --json >> pull.log 2>&1 # fuelrod-backup:pull"

    @pytest.mark.parametrize("bad", ["not a cron", "* * *", "a b c d e"])
    def test_invalid_cron(self, bad):
        with pytest.raises(ValueError):
            sched.render("pull", bad, self.CMD, "x.log", windows=False)

    @staticmethod
    def _flags(cron):
        return sched.render("push", cron, TestRender.CMD, "p.log", windows=True)[1]

    def test_windows_daily(self):
        argv = self._flags("0 4 * * *")
        assert argv[argv.index("/SC"):argv.index("/SC") + 4] == ["/SC", "DAILY", "/ST", "04:00"]

    def test_windows_hourly(self):
        argv = self._flags("15 */6 * * *")
        assert argv[argv.index("/SC"):argv.index("/SC") + 6] == ["/SC", "HOURLY", "/MO", "6", "/ST", "00:15"]

    def test_windows_weekly(self):
        argv = self._flags("30 3 * * 1,5")
        assert argv[argv.index("/SC"):argv.index("/SC") + 6] == ["/SC", "WEEKLY", "/D", "MON,FRI", "/ST", "03:30"]

    def test_windows_task_names_per_job(self):
        pull = sched.render("pull", "0 4 * * *", self.CMD, "p.log", windows=True)[1]
        backup = sched.render("backup", "0 4 * * *", self.CMD, "p.log", windows=True)[1]
        assert pull[pull.index("/TN") + 1] == "fuelrod-backup-pull"
        assert backup[backup.index("/TN") + 1] == "fuelrod-backup"

    def test_windows_falls_back_with_a_note_for_unsupported_schedules(self):
        text, argv = sched.render("pull", "0 4 1 * *", self.CMD, "p.log", windows=True)
        assert "defaulted" in text
        assert argv[argv.index("/SC"):argv.index("/SC") + 4] == ["/SC", "DAILY", "/ST", "02:00"]

    def test_windows_log_path_with_spaces_is_quoted(self):
        text, argv = sched.render("pull", "0 4 * * *", self.CMD, "C:/My Logs/pull.log", windows=True)
        assert 'C:/My Logs/pull.log' in argv[argv.index("/TR") + 1]
        assert '"C:/My Logs/pull.log"' in argv[argv.index("/TR") + 1]

    def test_default_log_is_absolute_and_per_job(self):
        from pathlib import Path

        a, b = sched.default_log("pull"), sched.default_log("push")
        assert Path(a).is_absolute() and a != b and a.endswith("fuelrod-backup-pull.log")


class FakeCrontab:
    def __init__(self, lines=()):
        self.lines = list(lines)

    def __call__(self, cmd, **kw):
        if cmd[:2] == ["crontab", "-l"]:
            return SimpleNamespace(returncode=0, stdout="\n".join(self.lines) + ("\n" if self.lines else ""))
        if cmd[:2] == ["crontab", "-"]:
            self.lines = kw["input"].splitlines()
            return SimpleNamespace(returncode=0, stdout="")
        raise AssertionError(cmd)


@pytest.fixture
def crontab(monkeypatch):
    fake = FakeCrontab(["MAILTO=me@example.com", "5 1 * * * /usr/bin/other-job"])
    monkeypatch.setattr(sched.subprocess, "run", fake)
    return fake


def _line(job, cron="0 1 * * *"):
    return sched.render(job, cron, ["/x/fuelrod-backup", job], f"{job}.log", windows=False)[0]


class TestCron:
    def test_jobs_are_installed_side_by_side_and_other_entries_are_kept(self, crontab):
        for job in ("backup", "pull", "push"):
            sched.install_cron(job, _line(job))
        assert "MAILTO=me@example.com" in crontab.lines and "5 1 * * * /usr/bin/other-job" in crontab.lines
        assert [j.job for j in sched.list_cron()] == ["backup", "pull", "push"]

    def test_reinstalling_a_job_replaces_only_that_job(self, crontab):
        sched.install_cron("pull", _line("pull", "0 1 * * *"))
        sched.install_cron("push", _line("push"))
        sched.install_cron("pull", _line("pull", "30 3 * * *"))
        pulls = [ln for ln in crontab.lines if "# fuelrod-backup:pull" in ln]
        assert len(pulls) == 1 and pulls[0].startswith("30 3 * * *")
        assert any("# fuelrod-backup:push" in ln for ln in crontab.lines)

    def test_the_entry_written_by_the_old_version_counts_as_the_backup_job(self, crontab):
        crontab.lines.append("0 2 * * * /x/fuelrod-backup backup --json >> b.log 2>&1 # fuelrod-backup")
        assert [(j.job, j.when) for j in sched.list_cron()] == [("backup", "0 2 * * *")]
        sched.install_cron("backup", _line("backup"))
        ours = [ln for ln in crontab.lines if "fuelrod-backup" in ln]
        assert len(ours) == 1

    def test_remove_one_job(self, crontab):
        sched.install_cron("pull", _line("pull"))
        sched.install_cron("push", _line("push"))
        assert sched.remove_cron("pull") is True
        assert [j.job for j in sched.list_cron()] == ["push"]
        assert sched.remove_cron("pull") is False

    def test_list_reports_when_and_what(self, crontab):
        sched.install_cron("pull", _line("pull", "30 3 * * *"))
        (job,) = sched.list_cron()
        assert job.when == "30 3 * * *" and "pull" in job.command


class TestWindowsTasks:
    CSV = "".join(
        f'"{name}","{when}","Ready"' + chr(10)
        for name, when in [
            (chr(92) + "fuelrod-backup", "25/11/2026 02:00:00"),
            (chr(92) + "fuelrod-backup-pull", "25/11/2026 03:30:00"),
            (chr(92) + "Other task", "1/1/2027 00:00:00"),
        ]
    )

    def test_list_finds_only_our_tasks(self, monkeypatch):
        monkeypatch.setattr(sched.subprocess, "run", lambda *a, **k: SimpleNamespace(returncode=0, stdout=self.CSV))
        assert [j.job for j in sched.list_tasks()] == ["backup", "pull"]

    def test_delete_reports_success_and_failure(self, monkeypatch):
        seen = []
        monkeypatch.setattr(
            sched.subprocess, "run", lambda cmd, **k: seen.append(cmd) or SimpleNamespace(returncode=0, stdout=""),
        )
        assert sched.delete_task("pull") is True and seen[0][3] == "fuelrod-backup-pull"
        monkeypatch.setattr(sched.subprocess, "run", lambda cmd, **k: SimpleNamespace(returncode=1, stdout=""))
        assert sched.delete_task("pull") is False
