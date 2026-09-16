"""
Replays the Environmental Sensor Telemetry dataset as a live Kafka stream.

Each CSV row is published to the 'sensor-readings' topic, keyed by device ID so
that readings from one sensor station always land in the same partition and keep
their order. The delay between messages is derived from the original timestamps
divided by SPEED_FACTOR, so the stream keeps the shape of the real data instead
of firing rows at a flat rate.

The producer is a one-shot job: it publishes the file and exits. It never talks
to MongoDB, which is what lets ingestion continue while the storage side is down.
"""

import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "sensor-readings")
PARTITIONS = int(os.getenv("TOPIC_PARTITIONS", "3"))
CSV_PATH = os.getenv(
    "CSV_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                 "iot_telemetry_data.csv"),
)
SPEED_FACTOR = float(os.getenv("SPEED_FACTOR", "1000"))
MAX_SLEEP = float(os.getenv("MAX_SLEEP", "2.0"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("producer")


def to_bool(value):
    """Convert the CSV's string booleans into real booleans.

    The dataset stores these fields as the text 'true' and 'false'. Python
    evaluates bool('false') as True, so the conversion has to be explicit or
    every reading would silently claim motion and light were detected.

    Args:
        value: The raw field value from the CSV.

    Returns:
        True only if the text reads 'true', ignoring case and surrounding space.
    """
    return str(value).strip().lower() == "true"


def parse_row(row):
    """Convert one raw CSV row into a typed reading ready to publish.

    Three quirks of the source data are handled here: timestamps arrive as Unix
    epoch floats in scientific notation, the boolean fields are strings, and
    temperature carries float32 rounding artefacts such as 19.700000762939453.

    Args:
        row: One row from csv.DictReader.

    Returns:
        A dict with typed fields and an ISO-8601 UTC timestamp.

    Raises:
        ValueError: If a numeric field cannot be parsed.
        KeyError: If an expected column is missing.
    """
    epoch = float(row["ts"])
    return {
        "timestamp": datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(),
        "epoch": epoch,
        "device": row["device"],
        "co": float(row["co"]),
        "humidity": round(float(row["humidity"]), 2),
        "lpg": float(row["lpg"]),
        "smoke": float(row["smoke"]),
        "temperature": round(float(row["temp"]), 2),  # float32 artefacts in source
        "light": to_bool(row["light"]),
        "motion": to_bool(row["motion"]),
    }


def wait_for_broker(retries=30, delay=2):
    """Open an admin connection, retrying until Kafka is reachable.

    This container can start before the broker finishes booting, so a failed
    first attempt is expected rather than fatal.

    Args:
        retries: How many attempts to make before giving up.
        delay: Seconds to wait between attempts.

    Returns:
        A connected KafkaAdminClient.

    Raises:
        SystemExit: If the broker is still unreachable after all retries.
    """
    for attempt in range(1, retries + 1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
            log.info("Connected to Kafka at %s", BOOTSTRAP)
            return admin
        except NoBrokersAvailable:
            log.warning("Kafka not reachable (attempt %d/%d), retrying...", attempt, retries)
            time.sleep(delay)
    log.error("Kafka unreachable at %s after %d attempts", BOOTSTRAP, retries)
    sys.exit(1)


def ensure_topic(admin):
    """Create the topic if it does not already exist.

    Automatic topic creation is disabled on the broker so that a typo fails
    loudly instead of quietly making an empty topic. Creating it here means a
    fresh clone of the repository runs with no manual setup step.

    Args:
        admin: A connected KafkaAdminClient.
    """
    try:
        admin.create_topics([
            NewTopic(name=TOPIC, num_partitions=PARTITIONS, replication_factor=1)
        ])
        log.info("Created topic '%s' with %d partitions", TOPIC, PARTITIONS)
    except TopicAlreadyExistsError:
        log.info("Topic '%s' already exists", TOPIC)


def main():
    """Publish the whole dataset to Kafka, then exit.

    Messages are keyed by device ID so each station's readings stay ordered
    within one partition. acks='all' makes the producer wait for the broker to
    confirm each write, and limiting in-flight requests to one keeps ordering
    intact if a retry fires. Malformed rows are logged and skipped rather than
    aborting the run.
    """
    admin = wait_for_broker()
    ensure_topic(admin)
    admin.close()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        linger_ms=50,
    )

    if not os.path.exists(CSV_PATH):
        log.error("Dataset not found at %s", CSV_PATH)
        sys.exit(1)

    sent = 0
    previous_epoch = None

    with open(CSV_PATH, newline="", encoding="UTF-8") as handle:
        for row in csv.DictReader(handle):
            try:
                reading = parse_row(row)
            except (ValueError, KeyError) as exc:
                log.warning("Skipping malformed row: %s", exc)
                continue

            if previous_epoch is not None:
                gap = (reading["epoch"] - previous_epoch) / SPEED_FACTOR
                if gap > 0:
                    time.sleep(min(gap, MAX_SLEEP))
            previous_epoch = reading["epoch"]

            producer.send(TOPIC, key=reading["device"], value=reading)
            sent += 1

            if sent % 1000 == 0:
                log.info("Published %d readings", sent)

    producer.flush()
    producer.close()
    log.info("Finished. Published %d readings in total.", sent)


if __name__ == "__main__":
    main()
