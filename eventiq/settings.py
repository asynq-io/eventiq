from typing import Any, Generic, Literal, TypeVar

from pydantic.networks import AnyUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

from .imports import ImportedType
from .types import Decoder, DefaultAction, Encoder


class BrokerSettings(BaseSettings):
    name: str = "default"
    description: str | None = None
    default_on_exc: DefaultAction = "nack"
    default_consumer_timeout: int = 300
    validate_error_delay: int | None = 3600 * 12

    model_config = SettingsConfigDict(env_prefix="BROKER_")


Url = TypeVar("Url", bound=AnyUrl | str)


class UrlBrokerSettings(BrokerSettings, Generic[Url]):
    url: Url
    connection_options: dict[str, Any] = {}


class ServiceSettings(BaseSettings):
    name: str
    title: str | None = None
    version: str = "0.1.0"
    description: str = ""
    asyncapi: Literal["3.0.0"] = "3.0.0"
    encoder: ImportedType[Encoder] | None = None
    decoder: ImportedType[Decoder] | None = None
