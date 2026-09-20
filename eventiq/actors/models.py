from typing import Literal

from eventiq.models import CloudEvent, D
from eventiq.types import ID


class ActorMessage(CloudEvent[D]):
    """Message exchanged with an `Actor`, carrying reply-correlation metadata."""

    kind: Literal["command", "query", "response"]
    conversation_id: ID
    reply_to: str | None = None
