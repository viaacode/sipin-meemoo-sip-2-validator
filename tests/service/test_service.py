from typing import Any
from threading import Thread
from queue import Queue
import json
from time import sleep

from cloudevents.events import EventOutcome
import pytest
import pulsar

from cloudevents import PulsarBinding
from testcontainers.core.container import DockerContainer

from app.app import EventListener


@pytest.fixture
def client(pulsar_config: dict[str, Any]) -> pulsar.Client:
    return pulsar.Client(f"pulsar://{pulsar_config['host']}:{pulsar_config['port']}")


@pytest.fixture
def producer(request: pytest.FixtureRequest, client: pulsar.Client, pulsar_config: dict[str, Any]) -> pulsar.Producer:
    # Pretends to be the unzip service
    producer = client.create_producer(pulsar_config["consumer_topic"])

    def remove_producer():
        producer.close()

    request.addfinalizer(remove_producer)
    return producer


@pytest.fixture
def consumer(request: pytest.FixtureRequest, client: pulsar.Client, pulsar_config: dict[str, Any]) -> pulsar.Consumer:
    # Pretends to be the transformator service
    consumer = client.subscribe(pulsar_config["producer_topic"], "test_subscriber")

    def remove_consumer():
        consumer.close()

    request.addfinalizer(remove_consumer)
    return consumer


def test_pulsar_container_running(pulsar_container: DockerContainer):
    exit_code, _ = pulsar_container.exec("pulsar version")
    assert exit_code == 0


def test_event_listener():
    event_listener = EventListener(timeout_ms=500)

    def task():
        sleep(1)
        event_listener.running = False

    thread = Thread(target=task)
    thread.start()
    event_listener.start_listening()
    thread.join()


def test_message(producer: pulsar.Producer, consumer: pulsar.Consumer):
    event_properties = {
        "id": "230622200554968796486122694453874671655",
        "source": "...",
        "specversion": "1.0",
        "type": "...",
        "datacontenttype": "application/json",
        "subject": "event subject",
        "time": "2022-05-18T16:08:41.356423+00:00",
        "outcome": "success",
        "correlation_id": "eac2ed9d37b4478d811daf7caa74f2db",
        "content_type": "application/cloudevents+json; charset=utf-8",
    }

    event_data = {
        "data": {
            "destination": "tests/sip-examples/2.1/film_standard_mkv",
        },
    }

    event_listener = EventListener(timeout_ms=200)
    queue: Queue[pulsar.Message] = Queue()

    def produce():
        sleep(0.1)
        producer.send(
            json.dumps(event_data).encode("utf-8"),
            properties=event_properties,
        )

    def consume():
        try:
            message = consumer.receive(timeout_millis=5000)
            queue.put(message)
        except pulsar.Timeout:
            pass
        finally:
            event_listener.running = False

    producer_thread = Thread(target=produce)
    consumer_thread = Thread(target=consume)
    consumer_thread.start()
    producer_thread.start()
    event_listener.start_listening()
    producer_thread.join()
    consumer_thread.join()

    message = queue.get_nowait()
    event = PulsarBinding.from_protocol(message)  # type: ignore

    print(event.get_data()["validation_report"])

    assert event.correlation_id == event_properties["correlation_id"]
    assert event.outcome == EventOutcome.SUCCESS
    assert "is_valid" in event.get_data()
    assert "sip_path" in event.get_data()
    assert event.get_data()["is_valid"]
