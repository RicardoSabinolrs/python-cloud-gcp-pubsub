from concurrent.futures import TimeoutError

from google.cloud import pubsub_v1
from mementos import MementoMetaclass

from app.crosscutting.utils import get_logger

logger = get_logger(__name__)


class Subscription(metaclass=MementoMetaclass):

    def __init__(self, config):
        self.project_id = config.application.project_id
        self.topic_id = config.application.topic_id
        self.subscription_id = config.application.subscription_id
        self.endpoint = config.application.endpoint

    def create_pull_subscription(self):
        """Create a new pull subscription on the given topic."""

        publisher = pubsub_v1.PublisherClient()
        subscriber = pubsub_v1.SubscriberClient()

        topic_path = publisher.topic_path(self.project_id, self.topic_id)
        subscription_path = subscriber.subscription_path(self.project_id, self.subscription_id)

        with subscriber:
            subscription = subscriber.create_subscription(
                request={
                    "name": subscription_path,
                    "topic": topic_path
                }
            )
        logger.debug("Subscription created: {}".format(subscription))

    def create_push_subscription(self):
        """Create a new push subscription on the given topic."""

        publisher = pubsub_v1.PublisherClient()
        subscriber = pubsub_v1.SubscriberClient()

        topic_path = publisher.topic_path(
            self.project_id,
            self.topic_id
        )
        subscription_path = subscriber.subscription_path(
            self.project_id,
            self.subscription_id
        )

        push_config = pubsub_v1.types.PushConfig(
            push_endpoint=self.endpoint
        )

        with subscriber:
            subscription = subscriber.create_subscription(
                request={
                    "name": subscription_path,
                    "topic": topic_path,
                    "push_config": push_config,
                }
            )

        logger.debug(f"Push subscription created: {subscription}.")
        logger.debug(f"Endpoint for subscription is: {endpoint}")

    def delete_subscription(self):
        """Deletes an existing Pub/Sub topic."""

        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(
            self.project_id,
            self.subscription_id
        )
        with subscriber:
            subscriber.delete_subscription(request={"subscription": subscription_path})

        logger.debug("Subscription deleted: {}".format(subscription_path))

    def receive_messages(self, timeout=None):
        """Receives messages from a pull subscription."""

        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(
            self.project_id,
            self.subscription_id
        )

        streaming_pull_future = subscriber.subscribe(
            self.subscription_path,
            callback=self._on_receive_callback
        )
        logger.debug(f"Listening for messages on {subscription_path}..\n")

        with subscriber:
            try:
                streaming_pull_future.result(timeout=timeout)
            except TimeoutError:
                streaming_pull_future.cancel()

    def receive_messages_with_flow_control(self, timeout=None):
        """Receives messages from a pull subscription with flow control."""
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(
            self.project_id,
            self.subscription_id
        )

        # Limit the subscriber to only have ten outstanding messages at a time.
        flow_control = pubsub_v1.types.FlowControl(max_messages=10)

        streaming_pull_future = subscriber.subscribe(
            subscription_path,
            callback=self._on_receive_callback,
            flow_control=flow_control
        )
        logger.debug(f"Listening for messages on {subscription_path}..\n")

        with subscriber:
            try:
                streaming_pull_future.result(timeout=timeout)
            except TimeoutError:
                streaming_pull_future.cancel()

    def _on_receive_callback(self, message):
        logger.debug(f"Received {message.data}.")
        message.ack()
