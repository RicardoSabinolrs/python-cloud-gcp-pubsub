import functools
import json

from google.cloud import pubsub_v1
from mementos import MementoMetaclass

from config import Configuration
from utils import get_logger, Stopwatch

LOGGER = get_logger(__name__)


class Publisher(metaclass=MementoMetaclass):

    def __init__(self):
        self.config = Configuration()
        self.project_id = self.config.application.project_id
        self.topic_name = self.config.application.topic_name
        self.pubsub = pubsub_v1.PublisherClient()

    def create_topic(self):
        """Create a new Pub/Sub topic."""
        topic_path = self._topic_path(self.topic_name)
        topic = self.pubsub.create_topic(topic_path)
        LOGGER.debug(f'Topic created: {topic}')
        return topic

    def delete_topic(self):
        """Deletes an existing Pub/Sub topic."""
        topic_path = self._topic_path(self.topic_name)
        self.pubsub.delete_topic(topic_path)
        LOGGER.debug(f'Topic deleted: {self.topic_path}')

    def publish_messages(self, events):
        """Publishes multiple messages to a Pub/Sub topic."""
        for _ in range(self.config.application.total_replication):
            for event in events:
                try:
                    with Stopwatch() as stopwatch:
                        """When you publish a message, the client returns a future."""
                        future = self._publish(data=event)

                        """Publish failures shall be handled in the callback function. 
                           Resolve the publish future in a separate thread
                        """
                        future.add_done_callback(
                            functools.partial(
                                self._on_published_callback
                            )
                        )
                    LOGGER.debug(
                        f"Scheduled message dispatch. "
                        f"Elapsed time: {stopwatch.elapsed:.3f} second(s)"
                    )
                except Exception as err:
                    LOGGER.exception(f"Error exception: {err}")

    def publish_messages_with_batch_settings(self, events):
        """Publishes multiple messages to a Pub/Sub topic with batch settings."""

        settings = {
            "max_bytes": self.config.application.max_bytes,  # default 100
            "max_latency": self.config.application.max_latency,  # default 1 MB
            "max_messages": self.config.application.max_messages,  # default 10 ms
        }
        batch_settings = pubsub_v1.types.BatchSettings(**settings)
        batch_publisher = pubsub_v1.PublisherClient(batch_settings)
        batch_topic_path = batch_publisher.topic_path(self.project_id, self.topic_name)

        for _ in range(self.config.application.total_replication):
            for event in events:
                try:
                    with Stopwatch() as stopwatch:
                        """ Non-blocking. Allow the publisher client to batch multiple messages"""
                        data = json.dumps(event).encode('utf-8')
                        future = batch_publisher.publish(batch_topic_path, data=data)

                        """Publish failures shall be handled in the callback function. 
                           Resolve the publish future in a separate thread
                        """
                        future.add_done_callback(
                            functools.partial(
                                self._on_published_callback
                            )
                        )
                    LOGGER.debug(
                        f"Scheduled message dispatch. "
                        f"Elapsed time: {stopwatch.elapsed:.3f} second(s)"
                        "Published messages with batch settings."
                    )
                except Exception as err:
                    LOGGER.exception(f"Error exception: {err}")

    def _publish(self, data):
        data = json.dumps(data).encode('utf-8')
        topic_path = self._topic_path(self.topic_name)
        return self.pubsub.publish(topic_path, data=data)

    def _topic_path(self, topic_name):
        return self.pubsub.topic_path(self.project_id, topic_name)

    def _on_published_callback(self, future):
        if future.exception(timeout=30):
            LOGGER.error(
                f'Publishing message on {self.topic_name} threw an '
                f'exception {future.exception()}'
            )

        message_id = future.result()
        LOGGER.debug(
            f"Finished sending event(s) to Topic | "
            f"Published message_id: {message_id}"
        )
