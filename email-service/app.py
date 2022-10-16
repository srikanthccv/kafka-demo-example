import json
import logging
import os

from random import randint
from confluent_kafka import Consumer, KafkaError, KafkaException
from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "email-topic")


consumer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "email-service",
    "auto.offset.reset": "smallest",
}

print("consumer_conf", consumer_conf)

consumer = Consumer(consumer_conf)

consumer = ConfluentKafkaInstrumentor().instrument_consumer(consumer)


def setup_opentelemetry():
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

    provider = TracerProvider(
        resource=Resource.create({"service.name": "email-service"})
    )
    trace.set_tracer_provider(provider)
    exporter = OTLPSpanExporter()
    span_processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(span_processor)




def consumer_forever(consumer, topics):
    try:
        consumer.subscribe(topics)
        running = True
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logging.error(
                        f"{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}\n"
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print(f"Received message: {msg.value().decode('utf-8')}")
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == "__main__":
    setup_opentelemetry()
    consumer_forever(consumer, [TOPIC])
