"""
Consumes sensor readings from Kafka, stores them in MongoDB and raises an alert
document whenever a metric crosses its configured threshold.

Delivery guarantee: at-least-once from Kafka (offsets are committed only after a
successful write), combined with deterministic document IDs and upserts in
MongoDB. A message processed twice therefore overwrites itself instead of
creating a duplicate, which makes the pipeline effectively exactly-once from the
point of view of the stored data.
"""

import json
import logging
import os
import sys
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "sensor-readings")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "sensor-consumer-v2")
MONGO_URI = os.getenv(
    "MONGO_URI", "mongodb://sensor:sensorpass@localhost:27017/?authSource=admin"
)
MONGO_DB = os.getenv("MONGO_DB", "sensordata")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))


THRESHOLDS_PATH = os.getenv(
    "THRESHOLDS_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "thresholds.json"),
)

with open(THRESHOLDS_PATH, encoding="utf-8-sig") as _f:
    _config = json.load(_f)
DEVICE_THRESHOLDS = _config["devices"]
DEFAULT_THRESHOLDS = _config["default"]
METRIC_LABELS = _config["labels"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("kafka").setLevel(logging.WARNING)
log = logging.getLogger("consumer")


def reading_id(reading):
    """Build the deterministic document ID for a reading.

    The same reading always maps to the same ID, so re-processing a message
    overwrites its own document rather than creating a duplicate.

    Args:
        reading: A parsed reading with 'device' and 'epoch' keys.

    Returns:
        A string of the form '<device>_<epoch>'.
    """
    return f"{reading['device']}_{reading['epoch']}"


def connect_mongo(retries=30, delay=2):
    """Open a MongoDB connection, retrying until the database is reachable.

    The container may start before MongoDB is ready, so a failed first attempt
    is expected rather than fatal.

    Args:
        retries: How many attempts to make before giving up.
        delay: Seconds to wait between attempts.

    Returns:
        A connected MongoClient.

    Raises:
        SystemExit: If the database is still unreachable after all retries.
    """
    for attempt in range(1, retries + 1):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
            client.admin.command("ping")
            log.info("Connected to MongoDB")
            return client
        except (ConnectionFailure, ServerSelectionTimeoutError):
            log.warning("MongoDB not reachable (attempt %d/%d), retrying...", attempt, retries)
            time.sleep(delay)
    log.error("MongoDB unreachable after %d attempts", retries)
    sys.exit(1)


def connect_kafka(retries=30, delay=2):
    """Subscribe to the Kafka topic, retrying until the broker is reachable.

    Auto-commit is disabled so that offsets advance only after a successful
    write to MongoDB. If this process dies mid-batch, Kafka redelivers those
    messages on restart instead of skipping them.

    Args:
        retries: How many attempts to make before giving up.
        delay: Seconds to wait between attempts.

    Returns:
        A subscribed KafkaConsumer.

    Raises:
        SystemExit: If the broker is still unreachable after all retries.
    """
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                group_id=GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                max_poll_records=BATCH_SIZE,
                api_version=(2, 6, 0),
            )
            log.info("Connected to Kafka at %s", BOOTSTRAP)
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka not reachable (attempt %d/%d), retrying...", attempt, retries)
            time.sleep(delay)
    log.error("Kafka unreachable after %d attempts", retries)
    sys.exit(1)


def setup_collections(db):
    """Create the indexes the end-user applications query on.

    Each index follows directly from an expected usage. Planners filter readings
    by station over a time range, or scan the whole network chronologically for
    dashboards. The citizen warning application asks for the most recent alerts
    at one station, and for anything severe currently active across the city.
    Index creation is idempotent, so this is safe to run on every start.

    Args:
        db: The MongoDB database handle.
    """
    db.readings.create_index([("device", ASCENDING), ("epoch", ASCENDING)])
    db.readings.create_index([("timestamp", ASCENDING)])
    db.alerts.create_index([("device", ASCENDING), ("epoch", DESCENDING)])
    db.alerts.create_index([("severity", ASCENDING), ("epoch", DESCENDING)])
    log.info("Indexes ensured on 'readings' and 'alerts'")


def severity(value, limit):
    """Grade how far past the threshold a reading sits.

    The warning application shows this to citizens, who need a plain indication
    of urgency rather than a raw measurement they cannot interpret.

    Args:
        value: The measured value.
        limit: The threshold it exceeded.

    Returns:
        'moderate' up to 25% above the threshold, 'high' up to 50%, and
        'severe' beyond that.
    """
    ratio = value / limit
    if ratio < 1.25:
        return "moderate"
    if ratio < 1.5:
        return "high"
    return "severe"


def find_breaches(reading):
    """Check a reading against its own station's thresholds.

    Each device is judged against its own baseline, so an alert means the value
    is unusual for that location rather than unusual across the whole network.
    Devices with no configured entry fall back to the default thresholds, which
    covers newly installed sensors without a code change.

    Each alert carries a station name, a plain-language metric label and a
    severity grade, so the citizen warning application can render it directly
    without interpreting raw sensor values.

    Args:
        reading: A parsed reading.

    Returns:
        A list of alert documents, one per metric exceeding its threshold.
        Empty if nothing was exceeded.
    """
    device = reading["device"]
    config = DEVICE_THRESHOLDS.get(device, DEFAULT_THRESHOLDS)
    known = device in DEVICE_THRESHOLDS
    limits = config["limits"]
    station = config["name"]

    alerts = []
    for metric, limit in limits.items():
        value = reading.get(metric)
        if value is not None and value > limit * 1.02:
            level = severity(value, limit)
            label = METRIC_LABELS.get(metric, metric)
            alerts.append({
                "_id": f"{reading_id(reading)}_{metric}",
                "device": device,
                "station": station,
                "timestamp": reading["timestamp"],
                "epoch": reading["epoch"],
                "metric": metric,
                "metric_label": label,
                "value": value,
                "threshold": limit,
                "exceedance": round(value - limit, 6),
                "severity": level,
                "baseline": "device" if known else "default",
                "message": f"{label.capitalize()} {level} at {station}",
            })
    return alerts


def main():
    """Run the consume-store-alert loop until interrupted.

    Each poll returns a batch of messages. Readings and any resulting alerts are
    written to MongoDB as bulk upserts, and only once those writes succeed are
    the Kafka offsets committed. That ordering is what makes the pipeline
    recoverable: a crash mid-batch loses no data, and the redelivered messages
    overwrite their own documents instead of duplicating them.
    """
    mongo = connect_mongo()
    db = mongo[MONGO_DB]
    setup_collections(db)

    consumer = connect_kafka()

    stored = 0
    raised = 0

    try:
        while True:
            batches = consumer.poll(timeout_ms=1000)
            if not batches:
                continue

            reading_ops = []
            alert_ops = []

            for records in batches.values():
                for record in records:
                    reading = record.value
                    reading["_id"] = reading_id(reading)
                    reading_ops.append(
                        UpdateOne({"_id": reading["_id"]}, {"$set": reading}, upsert=True)
                    )
                    for alert in find_breaches(reading):
                        alert_ops.append(
                            UpdateOne({"_id": alert["_id"]}, {"$set": alert}, upsert=True)
                        )

            if reading_ops:
                db.readings.bulk_write(reading_ops, ordered=False)
                stored += len(reading_ops)
            if alert_ops:
                db.alerts.bulk_write(alert_ops, ordered=False)
                raised += len(alert_ops)

            consumer.commit()

            if stored % 1000 < BATCH_SIZE:
                log.info("Stored %d readings, raised %d alerts", stored, raised)

    except KeyboardInterrupt:
        log.info("Stopping. Stored %d readings, raised %d alerts.", stored, raised)
    finally:
        consumer.close()
        mongo.close()


if __name__ == "__main__":
    main()
