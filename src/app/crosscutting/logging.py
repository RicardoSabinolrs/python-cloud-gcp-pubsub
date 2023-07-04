import logging

FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level="DEBUG", format=FORMAT)


def get_logger(package):
    logger = logging.getLogger(package)

    return logger
