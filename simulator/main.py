import json
import logging
import os
import signal
import threading
import time
from typing import Dict

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
)
LOGGER = logging.getLogger("traffic-simulator")

STOP_EVENT = threading.Event()

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:29092")
PRODUCE_INTERVAL_SECONDS = float(os.getenv("PRODUCE_INTERVAL_SECONDS", "0.5"))
PRODUCE_INTERVAL_HEALTHY_SECONDS = float(
    os.getenv("PRODUCE_INTERVAL_HEALTHY_SECONDS", PRODUCE_INTERVAL_SECONDS)
)
PRODUCE_INTERVAL_SLOW_SECONDS = float(
    os.getenv("PRODUCE_INTERVAL_SLOW_SECONDS", PRODUCE_INTERVAL_SECONDS)
)
PRODUCE_INTERVAL_STOPPED_SECONDS = float(
    os.getenv("PRODUCE_INTERVAL_STOPPED_SECONDS", PRODUCE_INTERVAL_SECONDS)
)
PRODUCE_INTERVAL_BURSTY_SECONDS = float(
    os.getenv("PRODUCE_INTERVAL_BURSTY_SECONDS", PRODUCE_INTERVAL_SECONDS)
)

HEALTHY_CONSUMER_MAX_POLL_RECORDS = int(
    os.getenv("HEALTHY_CONSUMER_MAX_POLL_RECORDS", "1200")
)
HEALTHY_CONSUMER_BATCH_SLEEP_SHORT_SECONDS = float(
    os.getenv("HEALTHY_CONSUMER_BATCH_SLEEP_SHORT_SECONDS", "0.2")
)
HEALTHY_CONSUMER_BATCH_SLEEP_LONG_SECONDS = float(
    os.getenv("HEALTHY_CONSUMER_BATCH_SLEEP_LONG_SECONDS", "1.5")
)
HEALTHY_CONSUMER_LONG_SLEEP_EVERY_BATCHES = int(
    os.getenv("HEALTHY_CONSUMER_LONG_SLEEP_EVERY_BATCHES", "4")
)

SLOW_CONSUMER_SLEEP_SECONDS = float(os.getenv("SLOW_CONSUMER_SLEEP_SECONDS", "8"))
SLOW_CONSUMER_MAX_POLL_RECORDS = int(
    os.getenv("SLOW_CONSUMER_MAX_POLL_RECORDS", "1")
)
STOPPED_CONSUMER_RUNTIME_SECONDS = float(os.getenv("STOPPED_CONSUMER_RUNTIME_SECONDS", "25"))

BURSTY_CONSUMER_MAX_POLL_RECORDS = int(
    os.getenv("BURSTY_CONSUMER_MAX_POLL_RECORDS", "400")
)
BURSTY_CONSUMER_FAST_SLEEP_SECONDS = float(
    os.getenv("BURSTY_CONSUMER_FAST_SLEEP_SECONDS", "0.05")
)
BURSTY_CONSUMER_SLOW_SLEEP_SECONDS = float(
    os.getenv("BURSTY_CONSUMER_SLOW_SLEEP_SECONDS", "2.2")
)
BURSTY_CONSUMER_CYCLE_SECONDS = float(
    os.getenv("BURSTY_CONSUMER_CYCLE_SECONDS", "60")
)
BURSTY_CONSUMER_FAST_SECONDS = float(
    os.getenv("BURSTY_CONSUMER_FAST_SECONDS", "24")
)

HEALTHY_TOPIC = os.getenv("HEALTHY_TOPIC", "demo.healthy")
SLOW_TOPIC = os.getenv("SLOW_TOPIC", "demo.slow")
STOPPED_TOPIC = os.getenv("STOPPED_TOPIC", "demo.stopped")
BURSTY_TOPIC = os.getenv("BURSTY_TOPIC", "demo.bursty")

HEALTHY_GROUP = os.getenv("HEALTHY_GROUP", "demo-healthy-group")
SLOW_GROUP = os.getenv("SLOW_GROUP", "demo-slow-group")
STOPPED_GROUP = os.getenv("STOPPED_GROUP", "demo-stopped-group")
BURSTY_GROUP = os.getenv("BURSTY_GROUP", "demo-bursty-group")


def install_signal_handlers() -> None:
    def _handler(signum, _frame):
        LOGGER.info("Received signal %s; stopping threads", signum)
        STOP_EVENT.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def build_producer() -> KafkaProducer:
    while not STOP_EVENT.is_set():
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda value: value.encode("utf-8"),
                acks=1,
                linger_ms=10,
                retries=3,
            )
            LOGGER.info("Connected Kafka producer to %s", BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            LOGGER.warning("Kafka not ready for producer yet, retrying in 2s")
            time.sleep(2)
    raise RuntimeError("Stop event set while building producer")


def build_consumer(topic: str, group: str, max_poll_records: int) -> KafkaConsumer:
    while not STOP_EVENT.is_set():
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id=group,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                max_poll_records=max_poll_records,
                consumer_timeout_ms=1000,
                session_timeout_ms=10000,
                heartbeat_interval_ms=3000,
            )
            LOGGER.info("Connected consumer group=%s topic=%s", group, topic)
            return consumer
        except NoBrokersAvailable:
            LOGGER.warning("Kafka not ready for consumer %s yet, retrying in 2s", group)
            time.sleep(2)
    raise RuntimeError("Stop event set while building consumer")


def producer_loop(topic: str, interval_seconds: float) -> None:
    producer = build_producer()
    seq = 0
    while not STOP_EVENT.is_set():
        payload: Dict[str, float] = {
            "seq": seq,
            "ts": time.time(),
            "topic": topic,
        }
        try:
            future = producer.send(topic, key=f"{topic}-{seq}", value=payload)
            future.get(timeout=10)
            seq += 1
        except KafkaError as error:
            LOGGER.error("Producer error on topic=%s: %s", topic, error)
            time.sleep(1)
        time.sleep(interval_seconds)

    producer.flush(timeout=5)
    producer.close(timeout=5)


def healthy_consumer_loop() -> None:
    consumer = build_consumer(
        HEALTHY_TOPIC,
        HEALTHY_GROUP,
        max_poll_records=HEALTHY_CONSUMER_MAX_POLL_RECORDS,
    )
    batch_count = 0
    while not STOP_EVENT.is_set():
        records = consumer.poll(timeout_ms=1000)
        count = sum(len(batch) for batch in records.values())
        if count > 0:
            consumer.commit()
            batch_count += 1

        # Simulate realistic micro-batching: mostly short pauses and occasional longer pauses.
        if batch_count > 0 and (
            batch_count % max(1, HEALTHY_CONSUMER_LONG_SLEEP_EVERY_BATCHES) == 0
        ):
            time.sleep(HEALTHY_CONSUMER_BATCH_SLEEP_LONG_SECONDS)
        else:
            time.sleep(HEALTHY_CONSUMER_BATCH_SLEEP_SHORT_SECONDS)

    consumer.close()


def slow_consumer_loop() -> None:
    consumer = build_consumer(
        SLOW_TOPIC,
        SLOW_GROUP,
        max_poll_records=SLOW_CONSUMER_MAX_POLL_RECORDS,
    )
    while not STOP_EVENT.is_set():
        records = consumer.poll(timeout_ms=1000)
        count = sum(len(batch) for batch in records.values())
        if count > 0:
            consumer.commit()
        time.sleep(SLOW_CONSUMER_SLEEP_SECONDS)

    consumer.close()


def stopped_consumer_loop() -> None:
    consumer = build_consumer(STOPPED_TOPIC, STOPPED_GROUP, max_poll_records=400)
    deadline = time.time() + STOPPED_CONSUMER_RUNTIME_SECONDS

    while not STOP_EVENT.is_set() and time.time() < deadline:
        records = consumer.poll(timeout_ms=1000)
        count = sum(len(batch) for batch in records.values())
        if count > 0:
            consumer.commit()
        time.sleep(0.2)

    LOGGER.info(
        "Stopped consumer group %s after %.1fs to simulate no active consumption",
        STOPPED_GROUP,
        STOPPED_CONSUMER_RUNTIME_SECONDS,
    )
    consumer.close()

    while not STOP_EVENT.is_set():
        time.sleep(5)


def bursty_consumer_loop() -> None:
    consumer = build_consumer(
        BURSTY_TOPIC,
        BURSTY_GROUP,
        max_poll_records=BURSTY_CONSUMER_MAX_POLL_RECORDS,
    )
    started = time.time()

    while not STOP_EVENT.is_set():
        elapsed = time.time() - started
        cycle_position = elapsed % max(1.0, BURSTY_CONSUMER_CYCLE_SECONDS)
        is_fast_phase = cycle_position < BURSTY_CONSUMER_FAST_SECONDS

        records = consumer.poll(timeout_ms=1000)
        count = sum(len(batch) for batch in records.values())
        if count > 0:
            consumer.commit()

        if is_fast_phase:
            time.sleep(BURSTY_CONSUMER_FAST_SLEEP_SECONDS)
        else:
            time.sleep(BURSTY_CONSUMER_SLOW_SLEEP_SECONDS)

    consumer.close()


def run_thread(name: str, target) -> threading.Thread:
    thread = threading.Thread(target=target, name=name, daemon=True)
    thread.start()
    return thread


def main() -> None:
    install_signal_handlers()

    producer_threads = [
        run_thread(
            "producer-healthy",
            lambda: producer_loop(HEALTHY_TOPIC, PRODUCE_INTERVAL_HEALTHY_SECONDS),
        ),
        run_thread("producer-slow", lambda: producer_loop(SLOW_TOPIC, PRODUCE_INTERVAL_SLOW_SECONDS)),
        run_thread(
            "producer-stopped",
            lambda: producer_loop(STOPPED_TOPIC, PRODUCE_INTERVAL_STOPPED_SECONDS),
        ),
        run_thread(
            "producer-bursty",
            lambda: producer_loop(BURSTY_TOPIC, PRODUCE_INTERVAL_BURSTY_SECONDS),
        ),
    ]

    # Allow producers to seed data so consumers create committed offsets quickly.
    time.sleep(5)

    consumer_threads = [
        run_thread("consumer-healthy", healthy_consumer_loop),
        run_thread("consumer-slow", slow_consumer_loop),
        run_thread("consumer-stopped", stopped_consumer_loop),
        run_thread("consumer-bursty", bursty_consumer_loop),
    ]

    all_threads = producer_threads + consumer_threads

    while not STOP_EVENT.is_set():
        if any(not thread.is_alive() for thread in all_threads):
            LOGGER.error("A simulator thread exited unexpectedly")
            STOP_EVENT.set()
            break
        time.sleep(1)

    for thread in all_threads:
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
