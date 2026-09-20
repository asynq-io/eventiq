# Middleware

## Base classes

::: eventiq.middleware.MiddlewareProtocol
    handler: python
    options:
        show_root_heading: true
        members_order: source
        show_signature_annotations: true

::: eventiq.middleware.Middleware
    handler: python
    options:
        show_root_heading: true
        members_order: source
        show_signature_annotations: true

## Background tasks

A middleware that needs to run work alongside the consumers - a poller, a probe,
a periodic flush - schedules it on the service rather than spawning a task
itself:

```python
class MetricsMiddleware(Middleware):
    async def after_broker_connect(self) -> None:
        self.service.start_background_task(self._report_forever, name="metrics")

    async def _report_forever(self) -> None:
        while True:
            await self._report()
            await anyio.sleep(30)
```

The task is cancelled once the consumers have stopped and before the broker
disconnects, so `after_broker_disconnect` always runs with it already finished.
Exceptions propagate and shut the service down, so a task that should survive
its own failures has to handle them itself.

See [`Service.start_background_task`](service.md).

---

## RetryMiddleware

::: eventiq.middlewares.retries.RetryMiddleware
    handler: python
    options:
        show_root_heading: true
        members_order: source
        show_signature_annotations: true

::: eventiq.middlewares.retries.MaxAge
    handler: python
    options:
        show_root_heading: true
        members_order: source
        show_signature_annotations: true

::: eventiq.middlewares.retries.MaxRetries
    handler: python
    options:
        show_root_heading: true
        members_order: source
        show_signature_annotations: true

::: eventiq.middlewares.retries.RetryWhen
    handler: python
    options:
        show_root_heading: true
        members_order: source
        show_signature_annotations: true

::: eventiq.middlewares.retries.expo
    handler: python
    options:
        show_root_heading: true
        show_signature_annotations: true

::: eventiq.middlewares.retries.constant
    handler: python
    options:
        show_root_heading: true
        show_signature_annotations: true

---

## DeadLetterQueueMiddleware

::: eventiq.middlewares.dlx.DeadLetterQueueMiddleware
    handler: python
    options:
        show_root_heading: true
        members_order: source
        show_signature_annotations: true

---

## ErrorHandlerMiddleware

::: eventiq.middlewares.error.ErrorHandlerMiddleware
    handler: python
    options:
        show_root_heading: true
        members_order: source
        show_signature_annotations: true

---

## HealthCheckMiddleware

::: eventiq.middlewares.healthcheck.HealthCheckMiddleware
    handler: python
    options:
        show_root_heading: true
        members_order: source
        show_signature_annotations: true
