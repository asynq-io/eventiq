from __future__ import annotations

import importlib
from types import ModuleType
from typing import TYPE_CHECKING, Annotated, Any, TypeVar

from pydantic import GetCoreSchemaHandler
from pydantic_core import PydanticCustomError, core_schema

AnyType = TypeVar("AnyType")


def import_from_string(path: str) -> Any:
    module_name, _, obj = path.partition(":")
    module = importlib.import_module(module_name)
    try:
        return getattr(module, obj)
    except AttributeError:
        raise ImportError(f"{module_name} has no object {obj}") from None


def import_string_validator(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return import_from_string(value)
        except ImportError as e:
            raise PydanticCustomError(
                "import_error", "Invalid python path: {error}", {"error": str(e)}
            ) from None
    else:
        # otherwise we just return the value and let the next validator do the rest of the work
        return value


if TYPE_CHECKING:
    ImportedType = Annotated[AnyType, ...]
else:

    class ImportedType:
        @classmethod
        def __class_getitem__(cls, item: AnyType) -> AnyType:
            return Annotated[item, cls()]

        @classmethod
        def __get_pydantic_core_schema__(
            cls, source: type[Any], handler: GetCoreSchemaHandler
        ) -> core_schema.CoreSchema:
            serializer = core_schema.plain_serializer_function_ser_schema(
                cls._serialize, when_used="json"
            )
            if cls is source:
                # Treat bare usage of ImportString (`schema is None`) as the same as ImportString[Any]
                return core_schema.no_info_plain_validator_function(
                    function=import_string_validator, serialization=serializer
                )
            else:
                return core_schema.no_info_before_validator_function(
                    function=import_string_validator,
                    schema=handler(source),
                    serialization=serializer,
                )

        @staticmethod
        def _serialize(v: Any) -> str:
            if isinstance(v, ModuleType):
                return v.__name__
            elif hasattr(v, "__module__") and hasattr(v, "__name__"):
                return f"{v.__module__}:{v.__name__}"
            else:
                return v

        def __repr__(self) -> str:
            return "ImportedType"
