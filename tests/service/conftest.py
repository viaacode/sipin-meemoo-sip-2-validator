from typing import Any
from time import sleep

import pytest
from testcontainers.core.container import DockerContainer

from viaa.configuration import ConfigParser


@pytest.fixture(scope="module")
def pulsar_config() -> dict[str, Any]:
    return ConfigParser().app_cfg["pulsar"]


@pytest.fixture(scope="module")
def pulsar_container(pulsar_config: dict[str, Any]) -> DockerContainer:
    return (
        DockerContainer("apachepulsar/pulsar")
        .with_command("bin/pulsar standalone")
        .with_bind_ports(6650, pulsar_config["port"])
    )


@pytest.fixture(scope="module", autouse=True)
def pulsar_setup_and_teardown(
    request: pytest.FixtureRequest,
    pulsar_container: DockerContainer,
    pulsar_config: dict[str, Any],
):
    pulsar_container.start()

    def remove_container():
        pulsar_container.stop()

    request.addfinalizer(remove_container)

    sleep(3)
    namespace = pulsar_config["consumer_topic"].rsplit("/", maxsplit=1)[0]
    if namespace.startswith("persistent://"):
        namespace = namespace[13:]
    code, result = pulsar_container.exec(f"pulsar-admin namespaces create {namespace}")
    print(result)
    assert code == 0
    code, result = pulsar_container.exec(
        f"pulsar-admin topics create {pulsar_config['consumer_topic']}"
    )
    print(result)
    assert code == 0
