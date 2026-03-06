import pytest

from eventiq.imports import ImportedType, import_from_string, import_string_validator


def test_import_from_string_valid():
    obj = import_from_string("eventiq:CloudEvent")
    from eventiq import CloudEvent

    assert obj is CloudEvent


def test_import_from_string_invalid_attr():
    with pytest.raises(ImportError, match="has no object"):
        import_from_string("eventiq:NonExistentClass")


def test_import_from_string_invalid_module():
    with pytest.raises(ModuleNotFoundError):
        import_from_string("non_existent_module:Something")


def test_import_string_validator_with_string():
    from eventiq import CloudEvent

    result = import_string_validator("eventiq:CloudEvent")
    assert result is CloudEvent


def test_import_string_validator_with_object():
    from eventiq import CloudEvent

    result = import_string_validator(CloudEvent)
    assert result is CloudEvent


def test_import_string_validator_invalid_path():
    from pydantic_core import PydanticCustomError

    with pytest.raises(PydanticCustomError):
        import_string_validator("eventiq:DoesNotExist")


def test_imported_type_repr():
    obj = ImportedType()
    assert repr(obj) == "ImportedType"


def test_imported_type_serialize_class():
    from eventiq import CloudEvent

    result = ImportedType._serialize(CloudEvent)
    assert "CloudEvent" in result
    assert "eventiq" in result


def test_imported_type_serialize_module():
    import eventiq

    result = ImportedType._serialize(eventiq)
    assert result == "eventiq"


def test_imported_type_serialize_other():
    result = ImportedType._serialize("just a string")
    assert result == "just a string"


# --- ImportedType.__class_getitem__ (line 47) ---


def test_imported_type_class_getitem():
    import typing
    from typing import get_args, get_origin

    result = ImportedType[str]
    # Should be Annotated[str, ImportedType()]
    assert get_origin(result) is typing.Annotated
    args = get_args(result)
    assert args[0] is str


# --- ImportedType.__get_pydantic_core_schema__ (lines 55-65) ---


def test_imported_type_schema_with_type():
    """Typed usage: ImportedType[SomeClass] → uses no_info_before_validator_function."""
    from pydantic import BaseModel

    from eventiq import CloudEvent

    class MyModel(BaseModel):
        handler: ImportedType[type[CloudEvent]]

    m = MyModel(handler="eventiq:CloudEvent")
    assert m.handler is CloudEvent


def test_imported_type_schema_bare():
    """Bare usage: ImportedType (no type arg) → uses no_info_plain_validator_function."""
    from pydantic import BaseModel

    class MyModel(BaseModel):
        handler: ImportedType

    m = MyModel(handler="eventiq:CloudEvent")
    from eventiq import CloudEvent

    assert m.handler is CloudEvent
