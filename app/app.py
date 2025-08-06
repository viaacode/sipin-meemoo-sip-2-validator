from pathlib import Path
import subprocess

from cloudevents.events import (
    Event,
    PulsarBinding,
    EventAttributes,
    EventOutcome,
)
from viaa.configuration import ConfigParser
from viaa.observability import logging
from meemoo_sip_validator.v2_1 import validate
import _pulsar

from app.services.pulsar import PulsarClient


APP_NAME = "meemoo-sip-2-validator"


class EventListener:
    """
    EventListener is responsible for listening to Pulsar events and processing them.
    """

    def __init__(self, timeout_ms: int | None = None):
        """
        Initializes the EventListener with configuration, logging, and Pulsar client.
        """
        config_parser = ConfigParser()
        self.log = logging.get_logger(__name__, config=config_parser)
        self.pulsar_config = config_parser.app_cfg["pulsar"]
        self.pulsar_client = PulsarClient(timeout_ms=timeout_ms)
        self.running = False

    def handle_incoming_message(self, event: Event):
        """
        Handles an incoming Pulsar event.

        Args:
            event (Event): The incoming event to process.
        """
        if not event.has_successful_outcome():
            self.log.info(f"Dropping non successful event: {event.get_data()}")
            return

        # Event attributes
        attributes = event.get_attributes()
        subject = attributes["subject"]

        # Event data
        incoming_event_data = event.get_data()
        destination = incoming_event_data["destination"]

        self.log.info(f"Start handling of {destination}.")

        unzipped_path = Path(destination)
        root_folder = self._get_single_subfolder(unzipped_path)

        # Shared cloudevents
        outgoing_attributes = None
        outgoing_event_data = {}

        if not root_folder:
            # Failed because the validation logic did not run
            outgoing_attributes = EventAttributes(
                source=APP_NAME,
                subject=subject,
                correlation_id=attributes["correlation_id"],
                outcome=EventOutcome.FAIL,
            )

            outgoing_event_data = {
                "is_valid": False,
                "validation_report": "",
                "sip_path": "",
                "sip_profile": "",
                "message": "There should be one single root folder in the ZIP file.",
            }

        else:
            is_valid, report = validate(root_folder)

            # Successful in the sense that it was possible to run the validation logic
            outgoing_attributes = EventAttributes(
                source=APP_NAME,
                subject=subject,
                correlation_id=attributes["correlation_id"],
                outcome=EventOutcome.SUCCESS,
            )

            outgoing_event_data = {
                "is_valid": is_valid,
                "validation_report": report,
                "sip_path": str(root_folder),
                "message": "The SIP has been validated",
            }

        outgoing_event = Event(outgoing_attributes, outgoing_event_data)

        self.pulsar_client.produce_event(
            topic=self.pulsar_config["producer_topic"], event=outgoing_event
        )

    def start_listening(self):
        """
        Starts listening for incoming messages from the Pulsar topic.
        """
        self.ensure_java_installed()

        # Allows stopping the service from another thread, usefull for testing.
        self.running = True
        while self.running:
            try:
                msg = self.pulsar_client.receive()
            except _pulsar.Timeout:
                continue

            try:
                event = PulsarBinding.from_protocol(msg)  # type: ignore
                self.handle_incoming_message(event)
                self.pulsar_client.acknowledge(msg)
            except Exception as e:
                # Catch and log any errors during message processing
                self.log.error(f"Error: {e}")
                self.pulsar_client.negative_acknowledge(msg)

        self.pulsar_client.close()

    def _get_single_subfolder(self, unzipped_path: Path) -> Path | None:
        subfolder = next(unzipped_path.iterdir(), None)
        is_subfolder = subfolder is not None and subfolder.is_dir() 
        if not is_subfolder:
            return None
        return subfolder

    def ensure_java_installed(self):
        _ = subprocess.run(["java", "--version"]) 
