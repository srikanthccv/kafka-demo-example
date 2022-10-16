import json
import os

from random import randint
from confluent_kafka import Producer
from flask import Flask
from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor
from opentelemetry.instrumentation.flask import FlaskInstrumentor

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "order-topic")

app = Flask(__name__)
producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

FlaskInstrumentor().instrument_app(app)
producer = ConfluentKafkaInstrumentor().instrument_producer(producer)




def setup_opentelemetry():
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

    provider = TracerProvider(
        resource=Resource.create({"service.name": "order-service"})
    )
    trace.set_tracer_provider(provider)
    exporter = OTLPSpanExporter(insecure=True)
    span_processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(span_processor)


@app.route("/")
def hello_world():
    order_num = randint(1, 10000000000)
    products = ["apple", "banana", "orange", "grape", "watermelon", "pineapple"]
    quantity = randint(1, 100)
    order_data = {"order_num": order_num, "products": products, "quantity": quantity}
    bytes_string = json.dumps(order_data).encode("utf-8")
    producer.produce(TOPIC, value=bytes_string)
    return f"Order {order_num} placed successfully!"


if __name__ == "__main__":
    setup_opentelemetry()
    app.run(host='0.0.0.0', port=9090)
