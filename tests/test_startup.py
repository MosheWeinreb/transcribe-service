"""Launcher checks with a fake Docker CLI; no daemon or database required."""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class StartupTests(unittest.TestCase):
    def test_startup_preserves_settings_and_stops_on_build_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory)
            calls = project / "calls.txt"
            windows = os.name == "nt"
            launcher = "run.ps1" if windows else "run.sh"
            (project / "scripts").mkdir()
            shutil.copyfile(ROOT / "scripts" / launcher, project / "scripts" / launcher)
            (project / ".env.example").write_text("APP_PORT=8999\n", encoding="utf-8")
            docker = project / ("docker.cmd" if windows else "docker")
            docker.write_text(
                "@echo off\n"
                'echo %*>> "%DOCKER_CALLS%"\n'
                'if "%~2"=="build" if "%FAIL_BUILD%"=="1" exit /b 1\n'
                'if "%~2"=="exec" echo 8000\n'
                'if "%~2"=="port" echo 127.0.0.1:8999\n'
                "exit /b 0\n"
                if windows
                else "#!/bin/sh\n"
                'printf "%s\\n" "$*" >> "$DOCKER_CALLS"\n'
                '[ "${2:-}" = build ] && [ "$FAIL_BUILD" = 1 ] && exit 1\n'
                '[ "${2:-}" = exec ] && printf "8000\\n"\n'
                '[ "${2:-}" = port ] && printf "127.0.0.1:8999\\n"\n'
                "exit 0\n",
                encoding="utf-8",
            )
            docker.chmod(0o755)
            command = (
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File"]
                if windows
                else ["sh"]
            ) + [str(project / "scripts" / launcher)]
            environment = {
                **os.environ,
                "PATH": str(project) + os.pathsep + os.environ["PATH"],
                "DOCKER_CALLS": str(calls),
                "FAIL_BUILD": "0",
            }
            first = subprocess.run(command, env=environment, capture_output=True, text=True)
            self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
            self.assertEqual((project / ".env").read_text(), "APP_PORT=8999\n")
            self.assertIn("http://127.0.0.1:8999", first.stdout)
            self.assertIn("compose up -d --wait", calls.read_text())

            (project / ".env").write_text("APP_PORT=9001\n", encoding="utf-8")
            calls.write_text("", encoding="utf-8")
            environment["FAIL_BUILD"] = "1"
            failed = subprocess.run(command, env=environment, capture_output=True, text=True)
            self.assertNotEqual(failed.returncode, 0)
            self.assertEqual((project / ".env").read_text(), "APP_PORT=9001\n")
            self.assertNotIn("compose up", calls.read_text())
            self.assertNotIn("App: http", failed.stdout)


if __name__ == "__main__":
    unittest.main()
