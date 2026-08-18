import json
from typing import Any

from fastapi import APIRouter, FastAPI
from fastapi.responses import HTMLResponse
from pydantic_asyncapi.v3 import AsyncAPI

from eventiq.asyncapi import get_async_api_spec
from eventiq.service import Service

ASYNCAPI_HTML = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>{title}</title>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link rel="icon" href="https://www.asyncapi.com/favicon.ico">
    </head>
    <body>
        <script src="https://unpkg.com/@asyncapi/web-component@latest/lib/asyncapi-web-component.js" defer></script>
        <asyncapi-component
          schemaUrl="{asyncapi_url}",
          config='{config}',
          cssImportPath="https://unpkg.com/@asyncapi/react-component@latest/styles/default.min.css">
        </asyncapi-component>
    </body>
    </html>
"""

_DEFAULT_CONFIG = {"show": {"info": True, "sidebar": True}}


def add_asyncapi_router(
    app: FastAPI,
    service: Service,
    prefix: str = "/asyncapi",
    config: dict[str, Any] | None = None,
    **kwargs: Any,
) -> None:
    config = config or _DEFAULT_CONFIG
    config_json = json.dumps(config)
    title = getattr(app, "title", "Async API")
    content = ASYNCAPI_HTML.format(
        asyncapi_url=f"{prefix}.json".removesuffix("/"),
        title=title,
        config=config_json,
    )
    _spec: AsyncAPI | None = None

    asyncapi_router = APIRouter(prefix=prefix, **kwargs)

    @asyncapi_router.get(
        "", name="get_asyncapi", include_in_schema=False, response_class=HTMLResponse
    )
    async def _() -> HTMLResponse:
        return HTMLResponse(content=content)

    @asyncapi_router.get(".json", name="get_asyncapi_json", include_in_schema=False)
    async def _() -> AsyncAPI:
        nonlocal _spec
        if _spec is None:
            _spec = get_async_api_spec(service)
        return _spec

    app.include_router(asyncapi_router)
