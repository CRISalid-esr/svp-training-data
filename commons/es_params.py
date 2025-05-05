import os

class ESParams:
    def __init__(self):
        self.user = os.getenv("ES_USER", "elastic")
        self.password = os.getenv("ES_PASSWORD", "elastic")
        self.host = os.getenv("ES_HOST", "localhost")
        self.port = os.getenv("ES_PORT", "9200")
        self.scheme = os.getenv("ES_SCHEME", "http")

    @property
    def url(self):
        # Construct the URL using the host and port
        return f"{self.scheme}://{self.host}:{self.port}"