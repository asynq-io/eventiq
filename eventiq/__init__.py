from importlib.metadata import version

from .broker import Broker
from .consumer import Consumer, ConsumerGroup, GenericConsumer
from .middleware import Middleware
from .models import CloudEvent, Publishes
from .service import Service

__version__ = version(__name__)

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
