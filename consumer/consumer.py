"""
Consumes sensor readings from Kafka, stores them in MongoDB, and tracks alert
episodes: contiguous periods where a metric exceeds its per-device threshold.

Delivery guarantee: at-least-once from Kafka (offsets are committed only after
successful writes), combined with deterministic document IDs and upserts in
MongoDB. A message processed twice therefore overwrites its own document
instead of creating a duplicate, which makes the pipeline effectively
exactly-once from the point of view of the stored data.

Alerting model: a metric flapping around its threshold does not produce one
alert per reading. Each device/metric pair has at most one OPEN episode at a
time: a breach opens it, further breaching readings extend it, and it closes
once the value drops meaningfully below the threshold again (hysteresis, not
the same line the breach used). A five-minute spike becomes one document with
a start time, an end time and a peak value, rather than one document per
polling interval.
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
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "sensor-consumer-v3")
MONGO_URI = os.getenv(
    "MONGO_URI", "mongodb://sensor:sensorpass@localhost:27017/?authSource=admin"
)
MONGO_DB = os.getenv("MONGO_DB", "sensordata")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))

OPEN_MARGIN = 1.02
CLOSE_MARGIN = 0.98

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


def station_config(device):
    """Look up a device's thresholds and display name.

    Falls back to the default configuration for a station not explicitly
    listed, which covers newly installed sensors without a code change.

    Args:
        device: The device ID from a reading.

    Returns:
        A tuple of (limits dict, station name, is_known bool).
    """
    config = DEVICE_THRESHOLDS.get(device, DEFAULT_THRESHOLDS)
    return config["limits"], config["name"], device in DEVICE_THRESHOLDS


def severity(value, limit):
    """Grade how far past the base threshold a value sits.

    Args:
        value: The measured value.
        limit: The base threshold (before the hysteresis margin).

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


def connect_mongo(retries=30, delay=2):
    """Open a MongoDB connection, retrying until the database is reachable.

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
    """Create the indexes the end-user applications, and the pipeline itself,
    query on.

    Args:
        db: The MongoDB database handle.
    """
    db.readings.create_index([("device", ASCENDING), ("epoch", ASCENDING)])
    db.readings.create_index([("timestamp", ASCENDING)])
    db.alert_episodes.create_index([("status", ASCENDING)])
    db.alert_episodes.create_index([("device", ASCENDING), ("start_epoch", DESCENDING)])
    db.alert_episodes.create_index([("status", ASCENDING), ("start_epoch", DESCENDING)])
    log.info("Indexes ensured on 'readings' and 'alert_episodes'")


def load_open_episodes(db):
    """Load every currently open episode into memory, keyed by device+metric.

    Called once at startup. This is what makes episode tracking survive a
    restart: whatever was last durably written to MongoDB becomes the seed
    state, and replayed readings are applied on top of it deterministically,
    landing on the same result they would have reached without the crash.

    Args:
        db: The MongoDB database handle.

    Returns:
        A dict mapping (device, metric) to the open episode document.
    """
    open_episodes = {}
    for doc in db.alert_episodes.find({"status": "open"}):
        open_episodes[(doc["device"], doc["metric"])] = doc
    if open_episodes:
        log.info("Resumed %d open episode(s) from a previous run", len(open_episodes))
    return open_episodes


def update_episodes(reading, open_episodes, pending_writes):
    """Apply one reading to the in-memory episode state for its device.

    For each metric with a configured threshold: if no episode is open and
    the reading clears the open margin, a new episode starts. If an episode
    is already open, the reading either extends it (still breaching, or
    sitting in the dead zone between the two margins) or closes it (dropped
    below the close margin). Every mutation is recorded in pending_writes,
    keyed by the episode's own ID, so a batch touching the same episode many
    times still produces one write per episode at the end of the batch.

    Args:
        reading: A parsed reading.
        open_episodes: The running dict of currently open episodes, mutated
            in place.
        pending_writes: Dict of episode _id to the document to upsert,
            mutated in place.
    """
    device = reading["device"]
    limits, station, known = station_config(device)

    for metric, limit in limits.items():
        value = reading.get(metric)
        if value is None:
            continue

        open_thr = limit * OPEN_MARGIN
        close_thr = limit * CLOSE_MARGIN
        key = (device, metric)
        episode = open_episodes.get(key)

        if episode is None:
            if value > open_thr:
                label = METRIC_LABELS.get(metric, metric)
                level = severity(value, limit)
                episode = {
                    "_id": f"{device}_{metric}_{reading['epoch']}",
                    "device": device,
                    "station": station,
                    "metric": metric,
                    "metric_label": label,
                    "status": "open",
                    "start_epoch": reading["epoch"],
                    "start_timestamp": reading["timestamp"],
                    "last_epoch": reading["epoch"],
                    "last_timestamp": reading["timestamp"],
                    "peak_value": value,
                    "peak_epoch": reading["epoch"],
                    "threshold": limit,
                    "severity": level,
                    "baseline": "device" if known else "default",
                    "message": f"{label.capitalize()} {level} at {station}",
                }
                open_episodes[key] = episode
                pending_writes[episode["_id"]] = dict(episode)
            continue

        if value < close_thr:
            episode["status"] = "closed"
            episode["end_epoch"] = reading["epoch"]
            episode["end_timestamp"] = reading["timestamp"]
            episode["duration_seconds"] = round(
                episode["end_epoch"] - episode["start_epoch"], 1
            )
            pending_writes[episode["_id"]] = dict(episode)
            del open_episodes[key]
        else:
            episode["last_epoch"] = reading["epoch"]
            episode["last_timestamp"] = reading["timestamp"]
            if value > episode["peak_value"]:
                episode["peak_value"] = value
                episode["peak_epoch"] = reading["epoch"]
                level = severity(value, limit)
                episode["severity"] = level
                episode["message"] = f"{episode['metric_label'].capitalize()} {level} at {station}"
            pending_writes[episode["_id"]] = dict(episode)


def main():
    """Run the consume-store-alert loop until interrupted.

    Each poll returns a batch of messages. Readings are written to MongoDB as
    bulk upserts; episode state is updated in memory and its changes written
    as a second bulk upsert. Offsets are committed only once both writes
    succeed, so a crash between polls loses nothing and a replay recomputes
    the same episode state it would have reached without the crash.
    """
    mongo = connect_mongo()
    db = mongo[MONGO_DB]
    setup_collections(db)
    open_episodes = load_open_episodes(db)

    consumer = connect_kafka()

    stored = 0
    episode_writes_total = 0

    try:
        while True:
            batches = consumer.poll(timeout_ms=1000)
            if not batches:
                continue

            reading_ops = []
            pending_writes = {}

            for records in batches.values():
                for record in records:
                    reading = record.value
                    reading["_id"] = reading_id(reading)
                    reading_ops.append(
                        UpdateOne({"_id": reading["_id"]}, {"$set": reading}, upsert=True)
                    )
                    update_episodes(reading, open_episodes, pending_writes)

            if reading_ops:
                db.readings.bulk_write(reading_ops, ordered=False)
                stored += len(reading_ops)

            if pending_writes:
                episode_ops = [
                    UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
                    for doc in pending_writes.values()
                ]
                db.alert_episodes.bulk_write(episode_ops, ordered=False)
                episode_writes_total += len(episode_ops)
            consumer.commit()

            if stored % 1000 < BATCH_SIZE:
                log.info(
                    "Stored %d readings, %d episode(s) currently open, %d episode write(s) so far",
                    stored, len(open_episodes), episode_writes_total,
                )

    except KeyboardInterrupt:
        log.info(
            "Stopping. Stored %d readings, %d episode(s) still open.",
            stored, len(open_episodes),
        )
    finally:
        consumer.close()
        mongo.close()


if __name__ == "__main__":
    main()
