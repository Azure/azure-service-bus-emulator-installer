import errno
import os
import select
import stat
import subprocess
import tempfile
import time
import unittest
from pathlib import Path


LAUNCHER = Path(__file__).resolve().parents[1] / "LaunchEmulator.sh"
EULA_PROMPT = "https://go.microsoft.com/fwlink/?linkid=2139274"
PASSWORD_PROMPT = (
    "Enter the password for the SQL Server (To be filled as per policy : "
    "https://learn.microsoft.com/en-us/sql/relational-databases/security/"
    "strong-passwords?view=sql-server-linux-ver16)"
)
WAIT_PROMPT = "Enter the wait interval for SQL Server to be ready (in seconds) [default: 15]: "
PORT_PROMPT = "Enter the emulator HTTP port for health-check and Management APIs [default: 5300]: "


class LaunchEmulatorTests(unittest.TestCase):
    def test_interactive_password_is_not_echoed(self):
        test_credential = "ValidPassword1!"

        with tempfile.TemporaryDirectory() as temp_dir:
            docker = Path(temp_dir) / "docker"
            docker.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            docker.chmod(docker.stat().st_mode | stat.S_IXUSR)

            master, slave = os.openpty()
            environment = os.environ.copy()
            environment["PATH"] = f"{temp_dir}{os.pathsep}{environment['PATH']}"
            process = subprocess.Popen(
                ["bash", str(LAUNCHER)],
                cwd=LAUNCHER.parent,
                env=environment,
                stdin=slave,
                stdout=slave,
                stderr=slave,
                close_fds=True,
            )
            os.close(slave)

            transcript = bytearray()
            try:
                self._write_after(master, transcript, EULA_PROMPT, "Y\n")
                self._write_after(
                    master, transcript, PASSWORD_PROMPT, f"{test_credential}\n"
                )
                self._write_after(master, transcript, WAIT_PROMPT, "\n")
                self._write_after(master, transcript, PORT_PROMPT, "\n")
                transcript.extend(self._read_to_exit(master, process))
            finally:
                os.close(master)
                if process.poll() is None:
                    process.kill()
                process.wait()

            output = transcript.decode(errors="replace")
            self.assertEqual(0, process.returncode, output)
            self.assertNotIn(test_credential, output)
            self.assertIn(
                f"{PASSWORD_PROMPT}\r\n\r\n{WAIT_PROMPT}",
                output,
            )
            self.assertIn(
                "Emulator Service and dependencies have been successfully launched!",
                output,
            )

    def _write_after(self, master, transcript, expected, value):
        transcript.extend(self._read_until(master, expected.encode()))
        os.write(master, value.encode())

    def _read_until(self, master, expected):
        output = bytearray()
        deadline = time.monotonic() + 10
        while expected not in output:
            if time.monotonic() >= deadline:
                self.fail(
                    f"Timed out waiting for {expected!r}. Output: "
                    f"{output.decode(errors='replace')}"
                )
            ready, _, _ = select.select([master], [], [], 0.1)
            if ready:
                try:
                    output.extend(os.read(master, 4096))
                except OSError as error:
                    if error.errno != errno.EIO:
                        raise
                    self.fail(
                        f"Launcher exited before {expected!r}. Output: "
                        f"{output.decode(errors='replace')}"
                    )
        return output

    def _read_to_exit(self, master, process):
        output = bytearray()
        deadline = time.monotonic() + 10
        while process.poll() is None:
            if time.monotonic() >= deadline:
                self.fail(f"Launcher did not exit. Output: {output.decode(errors='replace')}")
            ready, _, _ = select.select([master], [], [], 0.1)
            if ready:
                try:
                    output.extend(os.read(master, 4096))
                except OSError as error:
                    if error.errno != errno.EIO:
                        raise
                    break

        while True:
            ready, _, _ = select.select([master], [], [], 0)
            if not ready:
                break
            try:
                output.extend(os.read(master, 4096))
            except OSError as error:
                if error.errno != errno.EIO:
                    raise
                break
        return output


if __name__ == "__main__":
    unittest.main()
