import logging
import multiprocessing
import random
import time

from google.cloud import pubsub_v1
from mementos import MementoMetaclass

from config import Configuration
from utils import get_logger

LOGGER = get_logger(__name__)


class Subscription(metaclass=MementoMetaclass):

    def __init__(self):
        self.config = Configuration()
        self.project_id = self.config.application.project_id
        self.topic_name = self.config.application.topic_name
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()

    def list_subscriptions_in_topic(self):
        """Lists all subscriptions for a given topic."""
        topic_path = self.publisher.topic_path(project_id, topic_name)
        [LOGGER.debug(subscription) for subscription in self.publisher.list_topic_subscriptions(topic_path)]

    def list_subscriptions_in_project(self):
        """Lists all subscriptions in the current project."""

        project_path = self.subscriber.project_path(self.project_id)
        [LOGGER.debug(subscription.name) for subscription in self.subscriber.list_subscriptions(project_path)]
        self.subscriber.close()

    def create_subscription(self, subscription_name):
        """Create a new pull subscription on the given topic."""

        topic_path = self.subscriber.topic_path(project_id, topic_name)
        subscription_path = self.subscriber.subscription_path(
            project_id,
            subscription_name
        )
        subscription = self.subscriber.create_subscription(
            subscription_path,
            topic_path
        )

        LOGGER.debug("Subscription created: {}".format(subscription))
        self.subscriber.close()

    def create_push_subscription(self, subscription_name, endpoint):
        """Create a new push subscription on the given topic."""

        topic_path = self.subscriber.topic_path(project_id, topic_name)
        subscription_path = self.subscriber.subscription_path(
            project_id,
            subscription_name
        )
        push_config = pubsub_v1.types.PushConfig(push_endpoint=endpoint)
        subscription = self.subscriber.create_subscription(
            subscription_path,
            topic_path,
            push_config
        )

        LOGGER.debug("Push subscription created: {}".format(subscription))
        LOGGER.debug("Endpoint for subscription is: {}".format(endpoint))
        self.subscriber.close()

    def delete_subscription(self, subscription_name):
        """Deletes an existing Pub/Sub topic."""

        subscription_path = self.subscriber.subscription_path(
            project_id,
            subscription_name
        )
        self.subscriber.delete_subscription(subscription_path)

        LOGGER.debug("Subscription deleted: {}".format(subscription_path))
        self.subscriber.close()

    def update_subscription(self, subscription_name, endpoint):
        """
            Updates an existing Pub/Sub subscription's push endpoint URL.
            Note that certain properties of a subscription, such as
            its topic, are not modifiable.
        """
        subscription_path = self.subscriber.subscription_path(
            project_id,
            subscription_name
        )
        push_config = pubsub_v1.types.PushConfig(push_endpoint=endpoint)
        subscription = pubsub_v1.types.Subscription(
            name=subscription_path,
            push_config=push_config
        )

        update_mask = {"paths": {"push_config"}}

        self.subscriber.update_subscription(subscription, update_mask)
        result = self.subscriber.get_subscription(subscription_path)

        LOGGER.debug("Subscription updated: {}".format(subscription_path))
        LOGGER.debug("New endpoint for subscription is: {}".format(result.push_config))
        self.ubscriber.close()

    def receive_messages(self, subscription_name, timeout=None):
        """Receives messages from a pull subscription."""

        # The `subscription_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/subscriptions/{subscription_name}`
        subscription_path = self.subscriber.subscription_path(
            project_id,
            subscription_name
        )

        def callback(message):
            LOGGER.debug("Received message: {}".format(message))
            message.ack()

        streaming_pull_future = self.subscriber.subscribe(
            subscription_path,
            callback=callback
        )
        LOGGER.debug("Listening for messages on {}..\n".format(subscription_path))

        # Wrap subscriber in a 'with' block to automatically call close() when done.
        with subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.
                streaming_pull_future.result(timeout=timeout)
            except:  # noqa
                streaming_pull_future.cancel()

    def receive_messages_with_custom_attributes(self, subscription_name, timeout=None):
        """Receives messages from a pull subscription."""

        subscription_path = self.subscriber.subscription_path(
            project_id,
            subscription_name
        )

        def callback(message):
            LOGGER.debug("Received message: {}".format(message.data))
            if message.attributes:
                LOGGER.debug("Attributes:")
                for key in message.attributes:
                    value = message.attributes.get(key)
                    LOGGER.debug("{}: {}".format(key, value))
            message.ack()

        streaming_pull_future = self.subscriber.subscribe(
            subscription_path,
            callback=callback
        )
        LOGGER.debug("Listening for messages on {}..\n".format(subscription_path))

        # Wrap subscriber in a 'with' block to automatically call close() when done.
        with subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.
                streaming_pull_future.result(timeout=timeout)
            except:  # noqa
                streaming_pull_future.cancel()

    def receive_messages_with_flow_control(self, subscription_name, timeout=None):

        subscription_path = self.subscriber.subscription_path(
            project_id,
            subscription_name
        )

        def callback(message):
            LOGGER.debug("Received message: {}".format(message.data))
            message.ack()

        # Limit the subscriber to only have ten outstanding messages at a time.
        flow_control = pubsub_v1.types.FlowControl(max_messages=10)

        streaming_pull_future = self.subscriber.subscribe(
            subscription_path,
            callback=callback,
            flow_control=flow_control
        )
        LOGGER.debug("Listening for messages on {}..\n".format(subscription_path))

        # Wrap subscriber in a 'with' block to automatically call close() when done.
        with subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.
                streaming_pull_future.result(timeout=timeout)
            except:  # noqa
                streaming_pull_future.cancel()

    def synchronous_pull(self, subscription_name):
        """Pulling messages synchronously."""

        subscription_path = self.subscriber.subscription_path(
            self.project_id,
            subscription_name
        )

        NUM_MESSAGES = 3
        # The subscriber pulls a specific number of messages.
        response = self.subscriber.pull(subscription_path, max_messages=NUM_MESSAGES)

        ack_ids = []
        for received_message in response.received_messages:
            LOGGER.debug("Received: {}".format(received_message.message.data))
            ack_ids.append(received_message.ack_id)

        # Acknowledges the received messages so they will not be sent again.
        self.subscriber.acknowledge(subscription_path, ack_ids)
        LOGGER.debug(
            "Received and acknowledged {} messages. Done.".format(
                len(response.received_messages)
            )
        )

        self.subscriber.close()

    def synchronous_pull_with_lease_management(self, subscription_name):
        """Pulling messages synchronously with lease management"""

        subscription_path = self.subscriber.subscription_path(
            self.project_id,
            subscription_name
        )

        NUM_MESSAGES = 2
        ACK_DEADLINE = 30
        SLEEP_TIME = 10

        # The subscriber pulls a specific number of messages.
        response = self.subscriber.pull(subscription_path, max_messages=NUM_MESSAGES)

        multiprocessing.log_to_stderr()
        logger = multiprocessing.get_logger()
        logger.setLevel(logging.INFO)

        def worker(msg):
            """Simulates a long-running process."""
            RUN_TIME = random.randint(1, 60)
            logger.info(
                "{}: Running {} for {}s".format(
                    time.strftime("%X", time.gmtime()), msg.message.data, RUN_TIME
                )
            )
            time.sleep(RUN_TIME)

        # `processes` stores process as key and ack id and message as values.
        processes = dict()
        for message in response.received_messages:
            process = multiprocessing.Process(target=worker, args=(message,))
            processes[process] = (message.ack_id, message.message.data)
            process.start()

        while processes:
            for process in list(processes):
                ack_id, msg_data = processes[process]
                # If the process is still running, reset the ack deadline as
                # specified by ACK_DEADLINE once every while as specified
                # by SLEEP_TIME.
                if process.is_alive():
                    # `ack_deadline_seconds` must be between 10 to 600.
                    self.subscriber.modify_ack_deadline(
                        subscription_path,
                        [ack_id],
                        ack_deadline_seconds=ACK_DEADLINE,
                    )
                    logger.info(
                        "{}: Reset ack deadline for {} for {}s".format(
                            time.strftime("%X", time.gmtime()),
                            msg_data,
                            ACK_DEADLINE,
                        )
                    )

                # If the processs is finished, acknowledges using `ack_id`.
                else:
                    self.subscriber.acknowledge(subscription_path, [ack_id])
                    logger.info(
                        "{}: Acknowledged {}".format(
                            time.strftime("%X", time.gmtime()), msg_data
                        )
                    )
                    processes.pop(process)

            # If there are still processes running, sleeps the thread.
            if processes:
                time.sleep(SLEEP_TIME)

        LOGGER.debug(
            "Received and acknowledged {} messages. Done.".format(
                len(response.received_messages)
            )
        )

        self.subscriber.close()

    def listen_for_errors(self, subscription_name, timeout=None):
        """Receives messages and catches errors from a pull subscription."""

        subscription_path = self.subscriber.subscription_path(
            project_id,
            subscription_name
        )

        def callback(message):
            LOGGER.debug("Received message: {}".format(message))
            message.ack()

        streaming_pull_future = self.subscriber.subscribe(
            subscription_path,
            callback=callback
        )
        LOGGER.debug("Listening for messages on {}..\n".format(subscription_path))

        with subscriber:
            try:
                streaming_pull_future.result(timeout=timeout)
            except Exception as e:
                streaming_pull_future.cancel()
                LOGGER.debug(
                    "Listening for messages on {} threw an exception: {}.".format(
                        subscription_name, e
                    )
                )
