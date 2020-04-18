from google.cloud import pubsub_v1
from mementos import MementoMetaclass

from config import Configuration
from utils import get_logger

LOGGER = get_logger(__name__)


class IdentityAccessManagement(metaclass=MementoMetaclass):
    def __init__(self):
        self.config = Configuration()
        self.project_id = self.config.application.project_id
        self.topic_name = self.config.application.topic_name
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()

    def get_topic_policy(self):
        """Logger the IAM policy for the given topic."""

        topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        policy = self.publisher.get_iam_policy(topic_path)
        LOGGER.debug("Policy for topic {}:".format(topic_path))
        for binding in policy.bindings:
            LOGGER.debug("Role: {}, Members: {}".format(binding.role, binding.members))

    def get_subscription_policy(self, subscription_name):
        """Logger the IAM policy for the given subscription."""

        subscription_path = self.subscriber.subscription_path(self.project_id, subscription_name)
        policy = self.subscriber.get_iam_policy(subscription_path)
        LOGGER.debug("Policy for subscription {}:".format(subscription_path))
        for binding in policy.bindings:
            LOGGER.debug("Role: {}, Members: {}".format(binding.role, binding.members))
        self.subscriber.close()

    def set_topic_policy(self, topic_name):
        """Sets the IAM policy for a topic."""

        topic_path = self.publisher.topic_path(self.project_id, topic_name)
        policy = self.publisher.get_iam_policy(topic_path)
        policy.bindings.add(role="roles/pubsub.viewer", members=["allUsers"])
        policy.bindings.add(
            role="roles/pubsub.publisher", members=["group:cloud-logs@google.com"]
        )
        policy = self.publisher.set_iam_policy(topic_path, policy)

        LOGGER.debug("IAM policy for topic {} set: {}".format(topic_name, policy))

    def set_subscription_policy(self, subscription_name):
        """Sets the IAM policy for a topic."""

        subscription_path = self.publisher.subscription_path(self.project_id, subscription_name)
        policy = self.publisher.get_iam_policy(subscription_path)
        policy.bindings.add(role="roles/pubsub.viewer", members=["allUsers"])
        policy.bindings.add(
            role="roles/editor", members=["group:cloud-logs@google.com"]
        )
        policy = self.publisher.set_iam_policy(subscription_path, policy)

        LOGGER.debug(
            "IAM policy for subscription {} set: {}".format(
                subscription_name, policy
            )
        )
        self.publisher.close()

    def check_topic_permissions(self):
        """Checks to which permissions are available on the given topic."""

        topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        permissions_to_check = ["pubsub.topics.publish", "pubsub.topics.update"]
        allowed_permissions = self.publisher.test_iam_permissions(
            topic_path, permissions_to_check
        )

        LOGGER.debug(
            "Allowed permissions for topic {}: {}".format(
                topic_path, allowed_permissions
            )
        )

    def check_subscription_permissions(self, subscription_name):
        """Checks to which permissions are available on the given subscription."""

        subscription_path = self.subscriber.subscription_path(self.project_id, subscription_name)
        permissions_to_check = [
            "pubsub.subscriptions.consume",
            "pubsub.subscriptions.update",
        ]
        allowed_permissions = self.subscriber.test_iam_permissions(
            subscription_path, permissions_to_check
        )

        LOGGER.debug(
            "Allowed permissions for subscription {}: {}".format(
                subscription_path, allowed_permissions
            )
        )
        self.subscriber.close()
