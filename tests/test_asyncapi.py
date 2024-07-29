from eventiq.asyncapi import get_async_api_spec


def test_asyncapi_generation(service):
    spec = get_async_api_spec(service)
    assert spec.info.title == service.title
