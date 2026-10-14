import json
import os
import subprocess
import unittest
from pathlib import Path


TEMPLATE_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILES = (
    "docker-compose-default.yml",
    "docker-compose-standalone-emulator.yml",
    "docker-compose-with-containerized-app.yml",
)


class ComposeTemplateTests(unittest.TestCase):
    def test_websocket_port_defaults_to_5673(self):
        for compose_file in COMPOSE_FILES:
            with self.subTest(compose_file=compose_file):
                emulator = self._render_emulator(compose_file)
                self._assert_websocket_configuration(emulator, 5673)

    def test_websocket_port_is_configurable(self):
        for compose_file in COMPOSE_FILES:
            with self.subTest(compose_file=compose_file):
                emulator = self._render_emulator(
                    compose_file, SERVICEBUS_WEBSOCKET_PORT="15673"
                )
                self._assert_websocket_configuration(emulator, 15673)

    def _render_emulator(self, compose_file, **overrides):
        environment = os.environ.copy()
        environment.update(
            {
                "ACCEPT_EULA": "Y",
                "SQL_PASSWORD": "ComposeTestPassword1!",
                "SQL_WAIT_INTERVAL": "1",
                **overrides,
            }
        )
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(TEMPLATE_ROOT / compose_file),
                "config",
                "--format",
                "json",
            ],
            check=True,
            capture_output=True,
            env=environment,
            text=True,
        )
        return json.loads(result.stdout)["services"]["emulator"]

    def _assert_websocket_configuration(self, emulator, port):
        self.assertEqual(
            str(port), emulator["environment"]["SERVICEBUS_WEBSOCKET_PORT"]
        )
        websocket_ports = [
            mapping
            for mapping in emulator["ports"]
            if mapping["target"] == port and mapping["published"] == str(port)
        ]
        self.assertEqual(1, len(websocket_ports))
        self.assertEqual("tcp", websocket_ports[0]["protocol"])


if __name__ == "__main__":
    unittest.main()
