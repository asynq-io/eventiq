from typing import Any, TypedDict

from eventiq.models import Publishes
from eventiq.types import Decoder, Encoder, Parameter, RetryStrategy, Timeout


class ActorConsumerGroupOptions(TypedDict, total=False):
    timeout: Timeout
    tags: list[str]
    encoder: Encoder
    decoder: Decoder
    description: str
    concurrency: int
    retry_strategy: RetryStrategy
    publishes: list[Publishes]
    parameters: dict[str, Parameter]
    asyncapi_extra: dict[str, Any]
