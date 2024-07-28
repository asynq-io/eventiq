from typing import Any, Generic, Literal, Optional, TypeVar

from pydantic.networks import AnyUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

from .imports import ImportedType
from .types import Decoder, Encoder


class BrokerSettings(BaseSettings):
    default_consumer_timeout: int = 300
    description: Optional[str] = None
    encoder: Optional[ImportedType[Encoder]] = None
    decoder: Optional[ImportedType[Decoder]] = None
    validate_error_delay: Optional[int] = 3600 * 12

    model_config = SettingsConfigDict(env_prefix="BROKER_")


Url = TypeVar("Url", bound=AnyUrl)


class UrlBrokerSettings(BrokerSettings, Generic[Url]):
    url: Url
    connection_options: dict[str, Any] = {}


class ServiceSettings(BaseSettings):
    name: str
    title: Optional[str] = None
    version: str = "0.1.0"
    description: str = ""
    asyncapi: Literal["3.0.0"] = "3.0.0"
