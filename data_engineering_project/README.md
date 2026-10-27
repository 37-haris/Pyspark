# Data Engineering End-to-End Project: Real-Time Streaming Pipeline

A streaming pipeline that fetches random user data from an API, sends it through **Kafka**, processes it with **Spark Structured Streaming**, and stores it in **Cassandra**. **Airflow** orchestrates the producer, and everything runs in **Docker**.

Based on the article [Data Engineering End-to-End Project — 1](https://medium.com/@dogukannulu/data-engineering-end-to-end-project-1-7a7be2a3671) by Dogukan Ulu, with updated Docker images and bug fixes.

---

## Architecture

```
┌────────────────┐     ┌────────────────────┐     ┌──────────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ Random User API│ ──► │      Airflow       │ ──► │   Kafka (3 brokers)  │ ──► │ Spark Structured │ ──► │    Cassandra    │
│ randomuser.me  │     │ stream_to_kafka.py │     │  topic: random_names │     │    Streaming     │     │ spark_streaming │
│                │     │ DAG: random_people_│     │  coordinated by      │     │ spark_streaming  │     │  .random_names  │
│                │     │      names         │     │  ZooKeeper (zoo1)    │     │      .py         │     │                 │
└────────────────┘     └────────────────────┘     └──────────────────────┘     └──────────────────┘     └─────────────────┘
```

1. **Producer**: a Python script calls the Random User API every 10 seconds (for 2 minutes), keeps 9 fields, and sends each person as a JSON message to the Kafka topic `random_names`.
2. **Airflow** schedules the producer every day at 01:00 (and it can be triggered manually).
3. **Kafka** stores the messages on 3 brokers (replication factor 3) so no data is lost if a broker fails. **ZooKeeper** coordinates the brokers.
4. **Spark Structured Streaming** reads the topic continuously, parses the JSON into columns and writes each new row to Cassandra.
5. **Cassandra** stores the final data.

---

## Tech stack

| Component | Docker image | Version | Port |
|---|---|---|---|
| ZooKeeper | `confluentinc/cp-zookeeper` | 7.5.3 | 2181 |
| Kafka (×3) | `confluentinc/cp-kafka` | Confluent 7.5.3 (Apache Kafka 3.5) | 9092, 9093, 9094 |
| Kafka UI | `provectuslabs/kafka-ui` | 0.7.2 | [localhost:8888](http://localhost:8888) |
| Cassandra | `cassandra` | 4.1 | 9042 |
| Spark | `apache/spark` | 3.5.3 (Scala 2.12) | — |
| Airflow | `apache/airflow` | 2.10.3 (standalone) | [localhost:8080](http://localhost:8080) |
| Python libs | `kafka-python`, `requests` | — | — |
| Spark connectors | `spark-sql-kafka-0-10_2.12:3.5.3`, `spark-cassandra-connector_2.12:3.5.1` | — | — |

---

## Project structure

```
data_engineering_project/
├── docker-compose.yml          # all services (Kafka cluster, Kafka UI, Cassandra, Spark, Airflow)
├── dags/
│   ├── stream_to_kafka.py      # producer: Random User API -> Kafka
│   └── stream_to_kafka_dag.py  # Airflow DAG that runs the producer
├── spark/
│   └── spark_streaming.py      # Spark Structured Streaming: Kafka -> Cassandra
├── cassandra/
│   └── create_tables.cql       # keyspace + table creation
└── README.md
```

### Data model (Kafka message = Cassandra row)

| Field | Type | Example |
|---|---|---|
| `full_name` (primary key) | text | Mr. John Smith |
| `gender` | text | male |
| `location` | text | 1234, Main Street |
| `city` | text | Sydney |
| `country` | text | Australia |
| `postcode` | text | 2000 |
| `latitude` | float | -33.86 |
| `longitude` | float | 151.21 |
| `email` | text | john.smith@example.com |

---

## Prerequisites

- **Docker Desktop** running, with at least **6 GB of memory** allocated (Settings → Resources).
- Internet access (Docker images, Random User API, Spark connector download).
- Tested on macOS with Apple Silicon. All images also support Intel/amd64.

---

## How to run

### Step 1: Start all services

```bash
cd data_engineering_project
docker compose up -d
docker compose ps        # 8 containers should be "Up"
```

Wait about 2 minutes for Cassandra and Airflow to start.

- **Kafka UI**: http://localhost:8888 (cluster `local` with 3 brokers)
- **Airflow**: http://localhost:8080. User `admin`. If `admin` / `admin` does not work, get the generated password:
  ```bash
  docker exec -it airflow cat /opt/airflow/standalone_admin_password.txt
  ```

### Step 2: Create the Kafka topic

In Kafka UI: **Topics → Add a Topic**. Name `random_names`, partitions `1`, replication factor `3`.

Or from the terminal:

```bash
docker exec -it kafka1 kafka-topics --create --topic random_names \
  --partitions 1 --replication-factor 3 --bootstrap-server kafka1:19092

docker exec -it kafka1 kafka-topics --describe --topic random_names --bootstrap-server kafka1:19092
```

### Step 3: Create the Cassandra keyspace and table

```bash
docker exec -it cassandra cqlsh -f /scripts/create_tables.cql
docker exec -it cassandra cqlsh -e "DESCRIBE TABLE spark_streaming.random_names;"
```

(If you get `Connection refused`, Cassandra is still starting. Wait 30 seconds.)

### Step 4: Run the producer with Airflow

1. In Airflow, switch on the DAG **`random_people_names`**.
2. Click **▶ Trigger DAG** to run it now.
3. Each run lasts 2 minutes and sends 12 messages (one every 10 s).

Check the messages:

- Kafka UI: **Topics → random_names → Messages**
- or terminal:
  ```bash
  docker exec -it kafka1 kafka-console-consumer --topic random_names \
    --from-beginning --bootstrap-server kafka1:19092
  ```

### Step 5: Start the Spark streaming job

```bash
docker exec -it spark_master /opt/spark/bin/spark-submit \
  --master "local[2]" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  /opt/spark-apps/spark_streaming.py
```

The job keeps running and waits for new messages. Leave this terminal open, and stop it with `Ctrl + C`.

### Step 6: Check the data in Cassandra

In a second terminal:

```bash
docker exec -it cassandra cqlsh -e "SELECT full_name, country, city, email FROM spark_streaming.random_names;"
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM spark_streaming.random_names;"
```

Trigger the DAG again: the count increases by 1 every 10 seconds (real-time streaming end to end).

---

## Stop and clean up

```bash
docker compose stop          # stop containers (data kept)
docker compose down          # remove containers (Cassandra data kept in the volume)
docker compose down -v       # remove containers AND Cassandra data
```

---

## Changes compared to the original article

| Article | This project | Why |
|---|---|---|
| Puckel `docker-airflow` image (2019), separate compose file | `apache/airflow:2.10.3` in the same `docker-compose.yml` | Puckel image is unmaintained and breaks with current dependencies |
| `bitnami/spark:3` + manual jar download | `apache/spark:3.5.3` + `--packages` | Official image; connectors downloaded automatically with matching versions |
| Connector versions mixed (3.0.0 / 3.3.0, Scala 2.12 / 2.13) | All Spark 3.5 / Scala 2.12 | Mismatched versions cause runtime errors |
| External Docker network `airflow-kafka` | Single default network | All services are in one compose file |
| Kafka Connect + Schema Registry | Removed | Not used by the pipeline; saves memory |
| Same API record sent 12 times | New random user at each iteration | Bug: API was called only once before the loop |
| `postcode` as integer | `postcode` as text | Some postcodes contain letters (e.g. `M5V 2T6`) and crashed the producer |
| No streaming checkpoint | `checkpointLocation` set | Required by Spark for fault tolerance (no duplicates after restart) |
| `delimeter` option on the Kafka source | Removed | Typo, and the option does not exist for Kafka |
| `airflow.operators.python_operator`, `schedule_interval` | `airflow.operators.python`, `schedule` | Deprecated Airflow imports/arguments |

---

## Troubleshooting

| Problem | Solution |
|---|---|
| Airflow login fails | `docker exec -it airflow cat /opt/airflow/standalone_admin_password.txt`, or reset: `docker exec -it airflow airflow users reset-password --username admin --password admin` |
| DAG not visible in Airflow | Wait 1 minute (DAG folder scan). Check `docker logs airflow` for import errors |
| Airflow task fails with `NoBrokersAvailable` | Kafka brokers not ready yet: `docker compose ps`, then retrigger the DAG |
| `cqlsh` → `Connection refused` | Cassandra still starting. Wait 30–60 s |
| Spark: `Keyspace spark_streaming does not exist` | Run Step 3 first |
| Spark job slow on first launch | It downloads the connectors from Maven (about 1 min) |
| Containers keep restarting / very slow | Give Docker more memory (6–8 GB) |

---

## Key concepts

- **Kafka topic**: a named stream of messages (`random_names`). Producers write to it, consumers read from it.
- **Broker**: a Kafka server. With 3 brokers and **replication factor 3**, every message is copied on all brokers.
- **ZooKeeper**: coordinates the Kafka cluster (live brokers, controller election, partition leaders, metadata). It does not store the messages.
- **DAG** (Airflow): a workflow of tasks with a schedule. Here: one task, `kafka_data_stream`, daily at 01:00.
- **Structured Streaming**: Spark treats the Kafka topic as an infinite table, and each new message becomes a new row processed in micro-batches.
- **Checkpoint**: Spark saves which Kafka offsets were already processed, so a restart continues where it stopped.
- **Keyspace** (Cassandra): equivalent of a database in SQL.
