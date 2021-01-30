#!/usr/bin/env python3

from publisher.topic import Publisher
from utils import get_logger, file
from config import Configuration

config = Configuration()
LOGGER = get_logger(__name__)


def _get_all_events():
    json_files = file.find_all_json_files_in_local_disk()
    return [
        file.read_file_content(path) for path in json_files
    ]


def _get_event(file_name, local_folder='resources/data'):
    return list(
        file.read_file_content(
            f'{local_folder}/{file_name}.json'
        ))


if __name__ == '__main__':
    events = _get_all_events()

    event_publisher = Publisher()
    topic_name = event_publisher.create_topic(config.application.topic_id)
    event_publisher.publish_messages_with_batch_settings(events)
