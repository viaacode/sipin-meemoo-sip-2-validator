from time import sleep

import pytest
from testcontainers.core.container import DockerContainer

from viaa.configuration import ConfigParser

pulsar_config = ConfigParser().app_cfg["pulsar"]

pulsar_container = (
    DockerContainer("apachepulsar/pulsar")
    .with_command("bin/pulsar standalone")
    .with_bind_ports(6650, pulsar_config["port"])
)


@pytest.fixture(scope="module", autouse=True)
def pulsar_setup_and_teardown(request: pytest.FixtureRequest):
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
