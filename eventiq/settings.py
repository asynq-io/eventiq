from typing import Any, Generic, TypeVar

from pydantic.networks import AnyUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

from .types import DefaultAction, Timeout


class BrokerSettings(BaseSettings):
    name: str = "default"
    description: str | None = None
    default_on_exc: DefaultAction = "nack"
    default_consumer_timeout: Timeout = 300
    validate_error_delay: int = 3600 * 12

    model_config = SettingsConfigDict(env_prefix="BROKER_")


Url = TypeVar("Url", bound=AnyUrl | str)


class UrlBrokerSettings(BrokerSettings, Generic[Url]):
    url: Url
    connection_options: dict[str, Any] = {}
