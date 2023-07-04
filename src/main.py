from app.crosscutting.logging import get_logger
from app.infra.settings.config import Configuration
from app.streaming.publisher import Publisher

config = Configuration()
LOGGER = get_logger(__name__)

if __name__ == '__main__':
    event_publisher = Publisher()
    topic_name = event_publisher.create_topic(config.application.topic_id)
    event_publisher.publish_messages_with_batch_settings(events)
