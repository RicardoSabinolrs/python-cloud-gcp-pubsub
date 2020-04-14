import time


class Stopwatch(object):
    def __init__(self):
        self.start = None
        self.end = None
        self.elapsed = None

    def start(self):
        self.start = time.time()

    def stop(self):
        self.end = time.time()
        self.elapsed = self.end - self.start

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end = time.time()
        self.elapsed = self.end - self.start
