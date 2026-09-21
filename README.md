# Municipal Environmental Sensor Stream Pipeline

A stream processing pipeline that ingests environmental sensor readings through
Apache Kafka, stores them in MongoDB, and tracks alert episodes when a sensor
station exceeds its own baseline or stops reporting. Built for the IU course
*Project: Data Engineering* (DLBDSEDE02), Task 2.

## What the system does

A municipality operates environmental sensor stations across the city. Each
station reports temperature, humidity, carbon monoxide, LPG and smoke as a
continuous stream. The pipeline:

1. Publishes each reading to a Kafka topic, keyed by device ID.
2. Consumes the stream, stores every reading in MongoDB, and tracks alert
   episodes: contiguous periods where a metric breaches its threshold, or
   where a station stops reporting altogether.

Two groups of end users sit on top of the stored data. **Urban planners** query
the accumulated `readings` collection for long-term dashboards on air quality
and environmental conditions. **Citizens** are served by a warning application
reading the `alert_episodes` collection, which surfaces active and past
incidents shortly after they start.

The pipeline is the data layer of that larger system: it does not render
dashboards or push notifications itself, but it guarantees that the data those
applications depend on is complete, correctly typed and free of duplicates.

## From dataset to municipality

The system is a working pipeline exercised against a stand-in for live sensor
data, not a claim that a real 405,184-reading week already exists in some
city's archive. What that stand-in represents, concretely:

**Three sensors, not three hundred.** The source dataset contains readings
from exactly three physical devices. In the scenario, these represent an
early, partial rollout — consistent with the assignment's own framing that
"some of the sensors have been installed" while a fuller network is still
being built out. Three stations is enough to demonstrate per-station
thresholds, partitioned ordering and per-device staleness detection; it is
not enough to demonstrate horizontal scaling under real load, which is a
separate claim addressed through the architecture itself (partition count,
sharding key — see Scalability below) rather than through this dataset's size.

**One week, replayed fast, not lived through slowly.** The data spans 12–19
July 2020. Nothing about the pipeline depends on that date: Kafka, MongoDB
and the alerting logic are unaware of when a reading was originally recorded,
and the same code runs unchanged against a live feed from any year. The
producer's acceleration (`SPEED_FACTOR`) exists purely to make a week of data
runnable in minutes for testing and demonstration; a live deployment would
set it to 1 and consume readings in real time, exactly as they arrive.

**What a real deployment would add.** A genuine municipal rollout would run
more than three sensors, run continuously rather than for one sampled week,
and need the operational safeguards a downloaded dataset cannot exercise —
sensor commissioning, physical maintenance, and a clear chain of
responsibility for the infrastructure. The pipeline's job in this portfolio is
to prove the data-handling architecture is correct and resilient at the scale
available; the assignment's brief explicitly permits an open dataset standing
in for a live feed on exactly these terms.

## Architecture

```
 CSV dataset ──> producer ──> Kafka topic ──> consumer ──> MongoDB
                             (3 partitions)              ├── readings
                                                         └── alert_episodes
```

- **producer** replays the sample dataset as a live stream. The delay between
  messages is derived from the original timestamps, so the stream keeps the
  shape of the real data rather than firing rows at a flat rate.
- **Kafka** (KRaft mode, no ZooKeeper) buffers the stream in a durable,
  offset-based log. Three partitions, one per sensor station.
- **consumer** validates and stores each reading, evaluates it against
  per-device thresholds, and tracks whether each device is still reporting.
- **MongoDB** stores readings and alert episodes in two collections.

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

**Writes are idempotent, for readings and for alert state alike.** Each
reading's `_id` is derived deterministically from device ID and timestamp, and
every write is an upsert. Alert episodes are reloaded from MongoDB at startup
and rebuilt from there as readings are replayed, so a consumer that crashes
mid-incident resumes the same episode rather than losing it or opening a
duplicate. This was verified by killing the consumer mid-stream while the
producer continued publishing, then restarting it: final reading counts and
final episode counts matched an uninterrupted run exactly.

## Scalability and maintainability

**Scalability.** The topic is partitioned by device ID, so additional consumer
instances in the same group divide the partitions between them without code
changes. MongoDB shards on the same key if the reading volume outgrows a single
node. Nothing in the design assumes a single machine.

**Maintainability.** Alert thresholds, staleness limits and station names all
live in `consumer/thresholds.json`, not in code, so operators adjust them
without a redeployment. A `default` block applies to any device not
explicitly listed, so newly installed sensors are covered immediately.
MongoDB's document model absorbs additional metrics from future sensor
hardware without a schema migration — a requirement of the project, since the
structure of data from planned advanced sensors is not yet known.

## Indexing

Each index follows from an expected usage rather than from the shape of the
data:

| Collection | Index | Serves |
|---|---|---|
| `readings` | `device`, `epoch` | Planner dashboards: one station's history over a time range |
| `readings` | `timestamp` | Planner dashboards: network-wide trends in chronological order |
| `alert_episodes` | `status` | The consumer's own startup query: reload every open episode |
| `alert_episodes` | `device`, `start_epoch` desc | Warning application: most recent episodes at a given station |
| `alert_episodes` | `status`, `start_epoch` desc | Warning application / operations view: everything active right now |

Index creation runs on every consumer start and is idempotent.

## What planners analyze

The "urban planner" user story needs concrete examples to mean anything, and
each example needs an honest answer to whether one week of three sensors can
actually back it up.

**Comparing locations — well supported.** The clearest, most defensible use of
this data: are some parts of the city measurably worse than others?

```javascript
db.readings.aggregate([
  { $group: {
      _id: "$device",
      avg_co: { $avg: "$co" },
      avg_smoke: { $avg: "$smoke" },
      avg_temperature: { $avg: "$temperature" },
      readings: { $sum: 1 }
  }}
])
```

The three stations already show real, measurable separation — their
99th-percentile temperatures alone span 20.1–30.3 °C — so this comparison is a
genuine finding here, not just a demonstration of the query mechanism.

**Incident history — well supported.** How often did a station exceed safe
limits, and for how long?

```javascript
db.alert_episodes.aggregate([
  { $match: { metric: { $ne: "connectivity" } } },
  { $group: {
      _id: { station: "$station", metric: "$metric" },
      episode_count: { $sum: 1 },
      minutes_over_threshold: { $sum: { $divide: ["$duration_seconds", 60] } },
      worst_peak: { $max: "$peak_value" }
  }}
])
```

This runs directly against real output already produced by this pipeline — 47
episodes with genuine start times, end times and peaks — so it answers a real
question with real numbers, not a hypothetical one.

**Trends over time — mechanically supported, but the data can't back a real
conclusion.** A dashboard would plot a station's readings over time:

```javascript
db.readings.find(
  { device: "b8:27:eb:bf:9d:51" },
  { epoch: 1, temperature: 1, co: 1, _id: 0 }
).sort({ epoch: 1 })
```

The query works and the (`device`, `epoch`) index makes it fast. But "trend"
implies a direction sustained over time, and one week is a single sample —
there is no way to tell a real seasonal or monthly trend from ordinary
week-to-week noise. A live deployment accumulating months of data is what
would make this query meaningful rather than merely functional.

**Time-of-day patterns — weakly supported.** Whether pollution peaks at a
particular hour is a natural planner question:

```javascript
db.readings.aggregate([
  { $group: {
      _id: { $hour: { $toDate: { $multiply: ["$epoch", 1000] } } },
      avg_co: { $avg: "$co" }
  }},
  { $sort: { "_id": 1 } }
])
```

One week gives exactly seven samples per hour-of-day bucket — enough to run
the query, not enough to trust the result. A single unusual day would visibly
distort every bucket it touches.

**Sensor placement planning — not supported by this data at all.** Deciding
where the next batch of sensors should go needs population density, traffic
volume or land-use data alongside the readings. None of that exists in this
dataset, and no query against `readings` alone can supply it. This is a
genuine planner need a real deployment would have to source separately.

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

## Alert episodes

A metric flapping around its threshold does not produce one alert per reading.
Each device/metric pair has at most one **open** episode at a time:

- A reading clearing the threshold by at least 2% **opens** an episode (or
  extends it, if one is already open, updating the peak value and severity).
- A reading dropping at least 2% **below** the threshold **closes** it,
  recording a duration and final peak.
- A reading in between — within 2% of the threshold either way — changes
  nothing. Whatever state the episode was already in, it stays in.

That 4-percentage-point gap between the opening and closing lines is
hysteresis: it stops a value oscillating right at the threshold from opening
and closing an episode on every single reading. Without it, an early version
of this pipeline produced 9,302 documents for one week of data, several
hundred of them exceeding their threshold by less than one part in a million.
With episodes, the same week produces 47 — each one a real incident with a
start time, an end time, and how bad it got.

Thresholds themselves are the 99th percentile of each metric **per device**,
computed from the dataset itself. The three stations sit in measurably
different environments — their 99th-percentile temperatures are 20.1 °C,
23.6 °C and 30.3 °C — so a single pooled threshold would make one station
alert constantly while another never alerted at all. Alerting metrics are CO,
smoke, LPG and temperature; humidity is stored but not alerted on, as high
relative humidity is not in itself a hazard.

## Connectivity monitoring

A silent sensor looks identical to a healthy one unless the pipeline says
otherwise — a station that has stopped reporting produces no threshold
breaches, so an alerting system that only watches values would show it as
perfectly fine. Connectivity episodes close that gap: each device has its own
expected reporting rhythm, derived from the data itself (three times the
longest gap that device shows in a normal week), and a longer silence opens a
`connectivity` episode exactly like a value breach does, closed the instant a
reading from that device arrives again.

| Station | Longest gap in the data | Silence threshold |
|---|---|---|
| Station 1 – Riverside Park | 36.4s | 110s |
| Station 2 – Market Square | 40.2s | 120s |
| Station 3 – Industrial Estate | 14.8s | 45s |

**Why event time, not wall-clock time.** The pipeline's "current time" is the
newest reading epoch seen from any device, not the system clock. This matters
because the producer can replay a week of data in a few minutes — judged
against wall-clock seconds, every device would appear dead within moments of
starting. Judged against the data's own timestamps, a device is only stale
relative to how far other devices have progressed.

**A stated limitation.** Because the clock is driven by incoming data, this
mechanism can only detect one device going quiet while others keep reporting.
If every device stops at once — or Kafka itself goes down — there is no
"other" data to notice the gap against; that failure mode is covered under
Operations below, not by this check. It also means the sample dataset, by
construction, never triggers a connectivity episode at these thresholds: no
station in a healthy week ever goes silent for three times its own worst
natural gap. That absence is itself the correct result, not a shortfall — the
one place in the data where it comes close is Station 3's single 14.8-second
gap, which briefly setting its threshold below 15s (for demonstration only)
will correctly catch.

## Alert episode documents

A value-based episode, while still active:

```json
{
  "status": "open",
  "device": "00:0f:00:70:91:0a",
  "station": "Station 1 - Riverside Park",
  "metric": "co",
  "metric_label": "carbon monoxide",
  "severity": "severe",
  "start_timestamp": "2020-07-12T22:41:50.439466+00:00",
  "peak_value": 0.014420105304506959,
  "threshold": 0.009254,
  "message": "Carbon monoxide severe at Station 1 - Riverside Park"
}
```

The same episode once resolved — this is what a planner or an auditor would
actually query for:

```json
{
  "status": "closed",
  "start_timestamp": "2020-07-12T22:41:50.439466+00:00",
  "end_timestamp": "2020-07-13T00:36:34.623342+00:00",
  "duration_seconds": 6884.2,
  "peak_value": 0.014420105304506959,
  "threshold": 0.009254
}
```

Severity grades how far past the threshold the peak sits: `moderate` up to
25% above, `high` up to 50%, `severe` beyond that. Station names and metric
labels exist so the citizen warning application can render an alert directly,
without interpreting raw sensor values; the raw peak and threshold are
retained for planners and for auditing. Station names are illustrative,
standing in for the physical locations a municipality would assign to each
device.

## Running the pipeline

Requirements: Docker Desktop. Nothing else is installed locally — Kafka,
MongoDB, the producer and the consumer all run as containers.

```bash
git clone https://github.com/MnOuSs/sensor-stream-pipeline.git
cd sensor-stream-pipeline

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
  --authenticationDatabase admin sensordata
```

Then, at the `sensordata>` prompt:

```javascript
print('readings:', db.readings.countDocuments({}))
print('value episodes:', db.alert_episodes.countDocuments({metric:{$ne:'connectivity'}}))
print('connectivity episodes:', db.alert_episodes.countDocuments({metric:'connectivity'}))
```

Expected output: `405171`, `47`, `0`.

The 13-reading difference from the dataset's 405,184 rows is expected: the
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
| `KAFKA_GROUP_ID` | consumer | `sensor-consumer-v4` | Consumer group |
| `CSV_PATH` | producer | `/data/iot_telemetry_data.csv` | Dataset location |
| `SPEED_FACTOR` | producer | `10000` | Replay acceleration |
| `MONGO_URI` | consumer | see compose file | Database connection |
| `THRESHOLDS_PATH` | consumer | `/app/thresholds.json` | Threshold config |

The 2% hysteresis margins and the 3x staleness multiplier are deliberate
design constants rather than per-deployment tuning knobs, so they live in code
(`consumer.py`) rather than as environment variables.

Kafka exposes a second listener on `localhost:29092` for running scripts
directly against the broker from the host, which is useful during development.

## Operations and failure modes

The Reliability section above covers what happens when *this pipeline's own
processes* crash — a dead consumer, a restarted container. This section
covers the layer above that: what happens when the infrastructure underneath
it fails, and whose job it is to respond.

**If MongoDB's disk fails.** As deployed here, MongoDB is a single instance
writing to a single Docker volume. A disk failure on that volume loses
everything — every stored reading and every alert episode, with nothing to
recover from. This is a real gap in the current setup, not a solved problem,
and it is the most serious item in Known Constraints below. Operating this
for real would need MongoDB running as a replica set of at least three nodes,
so one node's disk failing still leaves two complete copies, plus a scheduled
backup to storage outside the cluster entirely. One property of the pipeline
already helps here: because every write is an idempotent upsert with a
deterministic ID, restoring a backup and replaying whatever Kafka still
retains since that backup's timestamp is a safe recovery procedure — it lands
on the same state as if nothing had failed, rather than creating duplicates.
Kafka's own retention window would need to comfortably exceed the backup
interval for that to work, which is a capacity-planning decision for whoever
runs the platform, not something the pipeline code controls.

**If the network drops — and which network.** "The network" means at least
three different things here, and each has a different owner and a different
failure story:

- *Sensor to Kafka.* A physical sensor losing its connection to the broker is
  outside this system's reach entirely — the pipeline cannot buffer a message
  it never received. Connectivity monitoring (above) detects the symptom, a
  station going quiet, but it cannot tell a broken network link apart from a
  broken sensor: both look identical from here. A real deployment would need
  the sensor hardware itself to buffer readings locally and forward them once
  the link returns, which is a firmware/edge concern, not something this
  repository's code can address.
- *Kafka to MongoDB, or to the consumer.* This is exactly what the existing
  architecture is built to absorb: the broker holds messages until the
  consumer or the database is reachable again, as demonstrated by the
  kill-and-restart test under Reliability. No new mechanism needed here —
  this is the system doing what it was designed to do.
- *Stored data to the planner dashboard or citizen app.* If those downstream
  applications lose their own connection to MongoDB, that is an availability
  problem for whoever operates them, not for this pipeline. This repository's
  responsibility ends at the data being correctly stored and queryable; what
  a consuming application does with a dropped connection is its own concern.

**Who operates what.** No single team owns this whole picture, and being
explicit about the boundary is itself part of a correct design:

| Layer | What it is | Who would run it | What this repository covers |
|---|---|---|---|
| Field sensors | Physical hardware across the city | Facilities / IoT operations | Not at all — simulated by the producer replaying a dataset |
| Kafka + MongoDB | The data platform | Platform / database operations | The configuration (`docker-compose.yml`), not day-to-day operation |
| Producer + consumer | The ingestion and alerting logic | Data engineering | Fully — this is the code in this repository |
| Planner and citizen apps | Downstream consumers of `readings` and `alert_episodes` | Separate application teams | Not at all — out of scope, as stated under What the system does |

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
- Connectivity monitoring can only detect one device going silent relative to
  others still reporting; it cannot distinguish "every sensor stopped" from
  "the whole pipeline stopped" (see Connectivity monitoring above).
- Credentials are in plain text in `docker-compose.yml`. Acceptable for a local
  prototype, but a deployed system would use a secrets manager.
- Kafka is pinned to 3.8.1. Version 3.9.0 contains a validation bug
  ([KAFKA-18281](https://issues.apache.org/jira/browse/KAFKA-18281)) that
  rejects a controller listener bound to `0.0.0.0` in a combined
  broker/controller node, which prevents the broker from starting.