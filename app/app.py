from cloudevents.events import Event, PulsarBinding, EventAttributes, EventOutcome, CEMessageMode
from viaa.configuration import ConfigParser
from viaa.observability import logging

from app.services.pulsar import PulsarClient

from meemoo_sip_validator.sip_validator import MeemooSIPValidator
from pathlib import Path

APP_NAME = "meemoo-sip-2-validator"


class EventListener:
    """
    EventListener is responsible for listening to Pulsar events and processing them.
    """

    def __init__(self):
        """
        Initializes the EventListener with configuration, logging, and Pulsar client.
        """
        config_parser = ConfigParser()
        self.log = logging.get_logger(__name__, config=config_parser)
        self.pulsar_client = PulsarClient()

    def handle_incoming_message(self, event: Event):
        """
        Handles an incoming Pulsar event.

        Args:
            event (Event): The incoming event to process.
        """
        if not event.has_successful_outcome():
            self.log.info(f"Dropping non successful event: {event.get_data()}")
            return
        incoming_event_data = event.get_data()

        self.log.info(f"Start handling of {event.get_attributes()['subject']}.")
        
        validator = MeemooSIPValidator()
        succes, report = validator.validate(Path(event.get_attributes()['subject']))
        
        attributes = EventAttributes(
            source=APP_NAME,
            subject=incoming_event_data["subject"],
            correlation_id=incoming_event_data["correlation_id"],
            outcome=EventOutcome.SUCCESS,
        )
        
        outgoing_event_data = {
            "outcome": succes,
            "validation_report": report,
            "sip_path": incoming_event_data["subject"]
        }

        outgoing_event = Event(attributes, outgoing_event_data)
        outgoing_pulsar_event = PulsarBinding.to_protocol(outgoing_event, CEMessageMode.STRUCTURED)
        
        self.pulsar_client.produce_event(topic="sipin.validate", event=outgoing_pulsar_event)

    def start_listening(self):
        """
        Starts listening for incoming messages from the Pulsar topic.
        """
        while True:
            msg = self.pulsar_client.receive()
            try:
                event = PulsarBinding.from_protocol(msg) # type: ignore
                self.handle_incoming_message(event)
                self.pulsar_client.acknowledge(msg)
            except Exception as e:
                # Catch and log any errors during message processing
                self.log.error(f"Error: {e}")
                self.pulsar_client.negative_acknowledge(msg)

        self.pulsar_client.close()
