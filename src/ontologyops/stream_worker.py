"""Kafka consumer for Docker/Redpanda real CDC. Manual commits follow SQLite commits."""
import json
import os
import signal
import sys
from pathlib import Path

from .paths import DEFAULT_DB
from .storage import connect
from .streaming import KafkaRecord, process_record, counts


def run(bootstrap: str, topic: str = "ontologyops.public.payment_events", db: Path = DEFAULT_DB,
        max_records: int = 0) -> dict:
    try:
        from confluent_kafka import Consumer
    except ImportError as exc:
        raise RuntimeError("Install kafka extra: pip install -e '.[kafka]'") from exc
    consumer = Consumer({"bootstrap.servers": bootstrap,
                         "group.id": "ontologyops-reference-consumer-v02",
                         "enable.auto.commit": False,
                         "auto.offset.reset": "earliest"})
    stop = False

    def stop_handler(_sig, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)
    seen = 0
    consumer.subscribe([topic])
    try:
        with connect(db) as con:
            while not stop and (max_records == 0 or seen < max_records):
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise RuntimeError(f"Kafka read error (not acknowledged): {msg.error()}")
                result = process_record(con, KafkaRecord(msg.topic(), msg.partition(), msg.offset(), msg.value(), msg.key()))
                # Broker offset commit is after the DB transaction. Retries are safe.
                consumer.commit(message=msg, asynchronous=False)
                seen += 1
                print(json.dumps({"result": result, "topic": msg.topic(),
                                  "partition": msg.partition(), "offset": msg.offset()}), flush=True)
            return {"consumed": seen, **counts(con)}
    finally:
        consumer.close()
