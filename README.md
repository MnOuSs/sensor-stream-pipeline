# Municipal Environmental Sensor Stream Pipeline

A stream processing pipeline that ingests environmental sensor readings through
Apache Kafka, stores them in MongoDB, and raises alerts when a sensor station
exceeds its own baseline. Built for the IU course *Project: Data Engineering*
(DLBDSEDE02), Task 2.

## What the system does

A municipality operates environmental sensor stations across the city. Each
station reports temperature, humidity, carbon monoxide, LPG and smoke as a
continuous stream. The pipeline:

1. Publishes each reading to a Kafka topic, keyed by device ID.
2. Consumes the stream, stores every reading in MongoDB, and writes an alert
   document whenever a metric crosses that device's threshold.

Two groups of end users sit on top of the stored data. **Urban planners** query
the accumulated `readings` collection for long-term dashboards on air quality
and environmental conditions. **Citizens** are served by a warning application
reading the `alerts` collection, which surfaces exceedances shortly after they
occur.

The pipeline is the data layer of that larger system: it does not render
dashboards or push notifications itself, but it guarantees that the data those
applications depend on is complete, correctly typed and free of duplicates.

## Architecture

```
 CSV dataset ──> producer ──> Kafka topic ──> consumer ──> MongoDB
                             (3 partitions)              ├── readings
                                                         └── alerts
```

- **producer** replays the sample dataset as a live stream. The delay between
  messages is derived from the original timestamps, so the stream keeps the
  shape of the real data rather than firing rows at a flat rate.
- **Kafka** (KRaft mode, no ZooKeeper) buffers the stream in a durable,
  offset-based log. Three partitions, one per sensor station.
- **consumer** validates and stores each reading, and evaluates it against
  per-device thresholds.
- **MongoDB** stores readings and alerts in two collections.

## Reliability

The requirement is that the system does not break when data is temporarily
inaccessible. Three mechanisms deliver that:

**Kafka decouples ingestion from storage.** If the consumer or the database is
unavailable, messages remain in the topic. The producer is unaffected and keeps
publishing.

**Offsets are committed only after a successful write.** Auto-commit is
disabled. If the consumer dies mid-batch, those offsets were never advanced, so
on restart Kafka redelivers from the last confirmed position. This gives
at-least-once delivery — nothing is lost.

**Writes are idempotent.** Each document's `_id` is derived deterministically
from device ID and timestamp (`device_epoch`), and every write is an upsert.
A message processed twice overwrites its own document instead of creating a
second one. At-least-once delivery combined with idempotent writes yields
exactly-once *storage*, which is the guarantee that matters for the end users.

This was verified by killing the consumer mid-stream (`docker compose kill
consumer`) after 8,500 readings while the producer continued publishing, then
restarting it. The final document counts were identical to an uninterrupted run.

## Scalability and maintainability

**Scalability.** The topic is partitioned by device ID, so additional consumer
instances in the same group divide the partitions between them without code
changes. MongoDB shards on the same key if the reading volume outgrows a single
node. Nothing in the design assumes a single machine.

**Maintainability.** Alert thresholds and station names live in
`consumer/thresholds.json`, not in code, so operators adjust them without a
redeployment. A `default` block applies to any device not explicitly listed, so
newly installed sensors are covered immediately. MongoDB's document model
absorbs additional metrics from future sensor hardware without a schema
migration — a requirement of the project, since the structure of data from
planned advanced sensors is not yet known.

## Indexing

Each index follows from an expected usage rather than from the shape of the
data:

| Collection | Index | Serves |
|---|---|---|
| `readings` | `device`, `epoch` | Planner dashboards: one station's history over a time range |
| `readings` | `timestamp` | Planner dashboards: network-wide trends in chronological order |
| `alerts` | `device`, `epoch` desc | Warning application: most recent alerts at a given station |
| `alerts` | `severity`, `epoch` desc | Warning application: severe alerts currently active anywhere |

Index creation runs on every consumer start and is idempotent.

## Sample data

The prototype uses the
[Environmental Sensor Telemetry Data](https://www.kaggle.com/datasets/garystafford/environmental-sensor-data-132k)
dataset (Stafford, 2020): 405,184 readings from three IoT devices over seven
days in July 2020.

The CSV is **not included in this repository**. Download it from Kaggle and
place it at:

```
data/iot_telemetry_data.csv
```

### Data handling notes

Three properties of the raw data required explicit handling in the producer:

- Timestamps are Unix epoch floats in scientific notation (`1.5945120943859746E9`)
  and are converted to ISO-8601 UTC.
- `light` and `motion` are the strings `"true"` / `"false"`. Python evaluates
  `bool("false")` as `True`, so these are cast explicitly rather than implicitly.
- Temperature carries float32 rounding artefacts (`19.700000762939453`) and is
  rounded to two decimals.

## Alert thresholds

Thresholds are the 99th percentile of each metric **per device**, computed from
the dataset itself. The three stations sit in measurably different environments
— their 99th-percentile temperatures are 20.1 °C, 23.6 °C and 30.3 °C — so a
single pooled threshold would make one station alert constantly while another
never alerted at all. Per-device baselines mean an alert signifies "unusual for
this location".

Alerting metrics are CO, smoke, LPG and temperature. Humidity is stored but not
alerted on, as a high relative humidity is not in itself a hazard.

A reading must exceed its threshold by at least 2% before an alert is raised.
Percentile-derived thresholds produce a dense band of readings sitting
fractionally above the line: without this margin, 38% of alerts (5,699 of
15,001) fall within 2% of their threshold, several hundred of them exceeding it
by less than one part in a million. Such alerts are arithmetically correct but
carry no information, and an alerting system that cries wolf is one citizens
learn to ignore. The margin trades a small loss of sensitivity for alerts that
mean something.

## Alert documents

Alerts are written so the citizen warning application can render them directly,
without interpreting raw sensor values:

```json
{
  "station": "Station 1 - Riverside Park",
  "message": "Carbon monoxide moderate at Station 1 - Riverside Park",
  "severity": "moderate",
  "metric_label": "carbon monoxide",
  "value": 0.009464507364341408,
  "threshold": 0.009254,
  "exceedance": 0.000211
}
```

Severity grades how far past the threshold a reading sits: `moderate` up to 25%
above, `high` up to 50%, `severe` beyond that. Station names are configured in
`consumer/thresholds.json` alongside the thresholds, so operators can rename a
station or register new hardware without touching code. The raw value and
threshold are retained for planners and for auditing; the station name, message
and severity exist so that a non-technical reader can act on the alert.

Station names are illustrative, standing in for the physical locations a
municipality would assign to each device.

## Running the pipeline

Requirements: Docker Desktop. Nothing else is installed locally — Kafka,
MongoDB, the producer and the consumer all run as containers.

```bash
git clone https://github.com/MnOuSs/Python_Data_Analytics.git
cd Python_Data_Analytics/Sensor_Stream_Pipeline

# place the Kaggle CSV at data/iot_telemetry_data.csv

docker compose up -d --build
```

The producer publishes the dataset and exits; the consumer runs continuously.
Follow progress with:

```bash
docker compose logs -f consumer
```

Check the stored results once the consumer stops reporting new readings:

```bash
docker compose exec mongodb mongosh -u sensor -p sensorpass \
  --authenticationDatabase admin sensordata \
  --eval "print(db.readings.countDocuments({}), db.alerts.countDocuments({}))"
```

Expected output: `405171 9302`.

The 13-document difference from the dataset's 405,184 rows is expected: the
source contains 13 duplicate device/timestamp pairs, which the deterministic
`_id` correctly collapses into single documents.

Tear down, including stored data:

```bash
docker compose down -v
```

## Configuration

All services are configured through environment variables in
`docker-compose.yml`.

| Variable | Service | Default | Purpose |
|---|---|---|---|
| `KAFKA_BOOTSTRAP` | both | `kafka:9092` | Broker address |
| `KAFKA_TOPIC` | both | `sensor-readings` | Topic name |
| `KAFKA_GROUP_ID` | consumer | `sensor-consumer-docker` | Consumer group |
| `CSV_PATH` | producer | `/data/iot_telemetry_data.csv` | Dataset location |
| `SPEED_FACTOR` | producer | `10000` | Replay acceleration |
| `MONGO_URI` | consumer | see compose file | Database connection |
| `THRESHOLDS_PATH` | consumer | `/app/thresholds.json` | Threshold config |

Kafka exposes a second listener on `localhost:29092` for running scripts
directly against the broker from the host, which is useful during development.

## Repository layout

```
.
├── docker-compose.yml
├── explore.ipynb            
├── data/                     
├── producer/
│   ├── producer.py
│   ├── requirements.txt
│   └── Dockerfile
└── consumer/
    ├── consumer.py
    ├── thresholds.json
    ├── requirements.txt
    └── Dockerfile
```

## Known constraints

- Single-broker Kafka with replication factor 1. Appropriate for a prototype;
  a production deployment would run at least three brokers with a replication
  factor of 3.
- Fixed `container_name` values keep the documented commands readable, but mean
  only one instance of the stack can run at a time on a given machine.
- Credentials are in plain text in `docker-compose.yml`. Acceptable for a local
  prototype, but a deployed system would use a secrets manager.
- Kafka is pinned to 3.8.1. Version 3.9.0 contains a validation bug
  ([KAFKA-18281](https://issues.apache.org/jira/browse/KAFKA-18281)) that
  rejects a controller listener bound to `0.0.0.0` in a combined
  broker/controller node, which prevents the broker from starting.
