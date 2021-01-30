import functools
import json

from google import api_core
from google.cloud import pubsub_v1
from mementos import MementoMetaclass

from utils import get_logger, Stopwatch

logger = get_logger(__name__)


class Publisher(metaclass=MementoMetaclass):

    def __init__(self, config):
        self.project_id = config.application.project_id
        self.topic_name = config.application.topic_id

    def create_topic(self):
        """Create a new Pub/Sub topic."""
        publisher = pubsub_v1.PublisherClient()
        topic_path = self._topic_path(self.topic_id)
        topic = publisher.create_topic(topic_path)
        logger.debug(f'Topic created: {topic}')
        return topic

    def delete_topic(self):
        """Deletes an existing Pub/Sub topic."""
        publisher = pubsub_v1.PublisherClient()
        topic_path = self._topic_path(self.topic_id)
        publisher.delete_topic(topic_path)
        logger.debug(f'Topic deleted: {self.topic_path}')

    def publish_messages(self, events):
        """Publishes multiple messages to a Pub/Sub topic."""
        publisher = pubsub_v1.PublisherClient()
        for event in events:
            try:
                with Stopwatch() as stopwatch:
                    """When you publish a message, the client returns a future."""
                    future = self._publish(
                        data=event,
                        publisher=publisher
                    )

                    """Publish failures shall be handled in the callback function. 
                       Resolve the publish future in a separate thread
                    """
                    future.add_done_callback(
                        functools.partial(
                            self._on_published_callback
                        )
                    )
                logger.debug(
                    f"Scheduled message dispatch. "
                    f"Elapsed time: {stopwatch.elapsed:.3f} second(s)"
                )
            except Exception as err:
                logger.exception(f"Error exception: {err}")

    def publish_messages_with_batch_settings(self, events):
        """Publishes multiple messages to a Pub/Sub topic with batch settings."""

        settings = {
            "max_bytes": self.config.application.max_bytes,  # default 100
            "max_latency": self.config.application.max_latency,  # default 1 MB
            "max_messages": self.config.application.max_messages,  # default 10 ms
        }

        custom_retry = api_core.retry.Retry(
            initial=0.250,  # seconds (default: 0.1)
            maximum=90.0,  # seconds (default: 60.0)
            multiplier=1.45,  # default: 1.3
            deadline=300.0,  # seconds (default: 60.0)
            predicate=api_core.retry.if_exception_type(
                api_core.exceptions.Aborted,
                api_core.exceptions.DeadlineExceeded,
                api_core.exceptions.InternalServerError,
                api_core.exceptions.ResourceExhausted,
                api_core.exceptions.ServiceUnavailable,
                api_core.exceptions.Unknown,
                api_core.exceptions.Cancelled,
            ),
        )

        batch_settings = pubsub_v1.types.BatchSettings(**settings)
        publisher = pubsub_v1.PublisherClient(batch_settings)
        batch_topic_path = publisher.topic_path(
            self.project_id,
            self.topic_name
        )

        for event in events:
            try:
                with Stopwatch() as stopwatch:
                    """ Non-blocking. Allow the publisher client to batch multiple messages"""
                    data = json.dumps(event).encode('utf-8')
                    future = publisher.publish(
                        topic=batch_topic_path,
                        data=data,
                        retry=custom_retry
                    )

                    """Publish failures shall be handled in the callback function. 
                       Resolve the publish future in a separate thread
                    """
                    future.add_done_callback(
                        functools.partial(
                            self._on_published_callback
                        )
                    )
                logger.debug(
                    f"Scheduled message dispatch. "
                    f"Elapsed time: {stopwatch.elapsed:.3f} second(s)"
                    "Published messages with batch settings."
                )
            except Exception as err:
                logger.exception(f"Error exception: {err}")

    def _publish(self, publisher, data):
        data = json.dumps(data).encode('utf-8')
        topic_path = self._topic_path(self.topic_name)
        return publisher.publish(topic_path, data=data)

    def _topic_path(self, topic_name):
        return self.publisher.topic_path(self.project_id, topic_name)

    def _on_published_callback(self, future):
        if future.exception(timeout=30):
            logger.error(
                f'Publishing message on {self.topic_name} threw an '
                f'exception {future.exception()}'
            )

        message_id = future.result()
        logger.debug(
            f"Finished sending event(s) to Topic | "
            f"Published message_id: {message_id}"
        )
