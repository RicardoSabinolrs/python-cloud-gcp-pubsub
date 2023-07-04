import os

from munch import Munch
from app.crosscutting.singleton import Singleton


class Configuration(metaclass=Singleton):
    application = Munch(
        project_id=os.environ.get("PROJECT_ID", "test"),
        max_bytes=int(os.environ.get("MAX_BYTES_PER_BATCH", "1024")),
        max_messages=int(os.environ.get("MAX_MESSAGES_PER_BATCH", "10")),
        max_latency=float(os.environ.get("MAX_LATENCY_TO_PUBLISH", "1")),
        topic_id=os.environ.get("TOPIC_ID", "test"),
        subscription_id=os.environ.get("SUBSCRIPTION_ID", "test"),
        endpoint=os.environ.get("ENDPOINT", "https://test.appspot.com/push")
    )
