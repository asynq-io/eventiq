# Broker

## Base classes

::: eventiq.broker.Broker
    handler: python
    options:
        show_root_heading: true
        members_order: source
        show_signature_annotations: true

::: eventiq.broker.UrlBroker
    handler: python
    options:
        show_root_heading: true
        members_order: source
        show_signature_annotations: true

::: eventiq.broker.BulkMessage
    handler: python
    options:
        show_root_heading: true
        members_order: source
        show_signature_annotations: true

---

## NATS / JetStream

::: eventiq.backends.nats
    handler: python
    options:
        show_root_heading: false
        members_order: source
        show_signature_annotations: true

---

## RabbitMQ

::: eventiq.backends.rabbitmq
    handler: python
    options:
        show_root_heading: false
        members_order: source
        show_signature_annotations: true

---

## Kafka

::: eventiq.backends.kafka
    handler: python
    options:
        show_root_heading: false
        members_order: source
        show_signature_annotations: true

---

## Redis

::: eventiq.backends.redis
    handler: python
    options:
        show_root_heading: false
        members_order: source
        show_signature_annotations: true

---

## Stub (Testing)

::: eventiq.backends.stub
    handler: python
    options:
        show_root_heading: false
        members_order: source
        show_signature_annotations: true
