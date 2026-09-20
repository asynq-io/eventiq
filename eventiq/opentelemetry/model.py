from pydantic import Field

from eventiq.models import CloudEvent, D


class TraceContextCloudEvent(CloudEvent[D], abstract=True):
    tracecontext: dict[str, str] = Field(
        default_factory=dict, description="Distributed tracing context"
    )

    @property
    def extra_span_attributes(self) -> dict[str, str]:
        return {}
