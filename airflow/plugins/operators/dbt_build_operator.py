from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from airflow.models import BaseOperator


class DbtBuildOperator(BaseOperator):
    def __init__(
        self,
        project_dir: str,
        command: str = "build",
        profiles_dir: str | None = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.project_dir = Path(project_dir)
        self.command = command
        self.profiles_dir = profiles_dir

    def execute(self, context):
        # Airflow base image may not include dbt packages.
        probe = subprocess.run(
            [sys.executable, "-c", "import dbt"],
            check=False,
            capture_output=True,
            text=True,
        )
        if probe.returncode != 0:
            self.log.info("dbt not found in Airflow runtime; installing dbt-postgres.")
            install = subprocess.run(
                [sys.executable, "-m", "pip", "install", "dbt-postgres"],
                check=False,
                capture_output=True,
                text=True,
            )
            if install.stdout:
                self.log.info(install.stdout)
            if install.returncode != 0:
                self.log.error(install.stderr)
                raise RuntimeError("Failed to install dbt-postgres in Airflow runtime.")

        command_parts = self.command.split()
        cmd = ["python", "-m", "dbt.cli.main", *command_parts, "--project-dir", str(self.project_dir)]
        if self.profiles_dir:
            cmd.extend(["--profiles-dir", self.profiles_dir])
        if command_parts[0] in {"build", "test"}:
            cmd.extend(["--fail-fast"])
        env = os.environ.copy()
        env["DBT_LOG_PATH"] = "/tmp/dbt_logs"
        env["DBT_TARGET_PATH"] = "/tmp/dbt_target"
        subprocess.run(["mkdir", "-p", env["DBT_LOG_PATH"], env["DBT_TARGET_PATH"]], check=False)
        self.log.info("Running command: %s", " ".join(cmd))
        completed = subprocess.run(cmd, check=False, capture_output=True, text=True, env=env)
        if completed.stdout:
            self.log.info(completed.stdout)
        if completed.returncode != 0:
            self.log.error(completed.stderr)
            raise RuntimeError(f"dbt {self.command} failed")
        return completed.stdout
