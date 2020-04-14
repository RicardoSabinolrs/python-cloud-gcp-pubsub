import os

from munch import Munch
from utils.singleton import Singleton


class Configuration(metaclass=Singleton):
    application = Munch(
        max_bytes=int(os.environ.get("MAX_BYTES_PER_BATCH", "1024")),
        max_messages=int(os.environ.get("MAX_MESSAGES_PER_BATCH", "10")),
        max_latency=float(os.environ.get("MAX_LATENCY_TO_PUBLISH", "1")),
        topic_name=os.environ.get("TOPIC_NAME", "test"),
        project_id=os.environ.get("PROJECT_ID", "test"),
        total_replication=int(os.environ.get("TOTAL_REPLICATION", "5"))
    )
