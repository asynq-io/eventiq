from importlib.metadata import PackageNotFoundError, version

from .broker import Broker
from .consumer import Consumer, ConsumerGroup, GenericConsumer
from .middleware import Middleware
from .models import CloudEvent, Publishes
from .service import Service

try:
    __version__ = version(__name__)
except PackageNotFoundError:
    # Importable from a source checkout, a vendored directory or a zipapp, where
    # no distribution metadata is installed.
    __version__ = "0.0.0"

__all__ = [
    "Broker",
    "CloudEvent",
    "Consumer",
    "ConsumerGroup",
    "GenericConsumer",
    "Middleware",
    "Publishes",
    "Service",
    "__version__",
]
