# PySpark Workshops

Hands-on workshops with **Apache Spark (PySpark)**: data exploration, joins, Spark SQL, machine learning with Spark MLlib, and a real-time data engineering pipeline.

## Architecture diagram

[![Architecture diagram of 37-haris/pyspark](https://gitdiagram.com/37-haris/pyspark/diagram.png)](https://gitdiagram.com/37-haris/pyspark?utm_source=readme&utm_medium=picture)

## Workshops

| # | Workshop | Topic | Dataset | Where |
|---|---|---|---|---|
| 1 | [Mushroom Classification](#workshop-1-mushroom-classification) | EDA, all join types, Spark SQL, binary classification | [UCI Mushroom (Kaggle)](https://www.kaggle.com/datasets/uciml/mushroom-classification) | `first_workshop.ipynb` |
| 2 | [Car Evaluation](#workshop-2-car-evaluation-multiclass-classification) | Multiclass classification, imbalanced classes | [Car Evaluation](https://github.com/muttinenisairohith/Datasets/blob/6e7bec3b9cbdc25da1055472c837ef8a10f569ed/data/car_data.csv) | `second_workshop.ipynb` |
| 3 | [Real-Time Streaming Pipeline](#workshop-3-data-engineering-end-to-end-streaming-pipeline) | Airflow, Kafka, Spark Structured Streaming, Cassandra, Docker | [Random User API](https://randomuser.me) | `data_engineering_project/` |

➡️ New here? Start with the **[Step-by-step guide](#step-by-step-guide)**.

---

## Project structure

```
PySpark/
├── README.md                     # this file
├── pyproject.toml / uv.lock      # Python environment (uv)
├── first_workshop.ipynb          # Workshop 1: Mushroom classification
├── second_workshop.ipynb         # Workshop 2: Car evaluation
├── testing.ipynb                 # Warm-up: linear regression on California Housing
├── data/
│   ├── car_data.csv              # Workshop 2 dataset
│   └── CaliforniaHousing/        # warm-up dataset
└── data_engineering_project/     # Workshop 3 (Docker project, has its own README)
    ├── docker-compose.yml
    ├── dags/
    ├── spark/
    ├── cassandra/
    └── README.md
```

---

## Setup (workshops 1 and 2)

**Requirements**
- Python 3.14 and [uv](https://docs.astral.sh/uv/)
- **Java 17** (needed by Spark): `brew install openjdk@17`
- Dependencies (from `pyproject.toml`): `pyspark`, `jupyter`, `pandas`, `numpy`, `matplotlib`, `seaborn`, `kagglehub`

**Install and start Jupyter**

```bash
cd ~/Documents/PySpark
uv sync                     # creates .venv and installs the dependencies
source .venv/bin/activate
jupyter notebook            # or open the notebooks in VS Code
```

If Spark cannot find Java, add this at the top of the notebook:

```python
import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
```

---

## Step-by-step guide

Follow these steps in order. Each step says **what to do** and **what you should see**.

### Step 0: Prepare your computer (one time only)

1. **Install Java 17** (Spark runs on Java):
   ```bash
   brew install openjdk@17
   ```
2. **Install uv** (Python package manager), if you don't have it:
   ```bash
   brew install uv
   ```
3. **Install the Python dependencies**:
   ```bash
   cd ~/Documents/PySpark
   uv sync
   source .venv/bin/activate
   ```
   ✅ You should see `(my-pyspark)` at the start of your terminal prompt.
4. **Check that PySpark works**:
   ```bash
   python -c "import pyspark; print(pyspark.__version__)"
   ```
   ✅ It prints a version number (4.x).
5. **For workshop 3 only:** install **Docker Desktop**, start it, and give it at least **6 GB of memory** (Settings → Resources).

---

### Workshop 1: Mushroom Classification, step by step

| Step | What to do | What you should see |
|---|---|---|
| 1 | Open `first_workshop.ipynb` in Jupyter or VS Code and select the `.venv` kernel | The notebook opens and the kernel is ready |
| 2 | Run the first cell (`kagglehub.dataset_download`) | `Path to dataset files: ...` (the dataset is downloaded automatically) |
| 3 | Run the **SparkSession** and **loading** cells | `Rows: 8,124 | Columns: 23` and the schema (all `string`) |
| 4 | **2.1 Target distribution** | e ≈ 51.8%, p ≈ 48.2%: balanced classes |
| 5 | **2.2 Distinct values** | `veil_type` has only 1 value (it will be dropped) |
| 6 | **2.3 Missing values** | `stalk_root`: 2,480 missing (`?`), 30.53% |
| 7 | **2.4 Duplicates** | `Duplicate rows: 0` |
| 8 | **2.5 Odor vs class** (crosstab) | 8 odors belong to only one class; only `n` (none) is mixed |
| 9 | **Part 3: Cleaning** | `Dropped: ['veil_type']`, 22 columns left |
| 10 | **4.0 Lookup table** | Table of odor codes → labels |
| 11 | **4.1 LEFT JOIN** | Same row count before and after (8,124) |
| 12 | **4.2 Decode every column** | Readable table (`edible`, `foul`, `woods`…) |
| 13 | **4.3 INNER / LEFT / RIGHT** | INNER has fewer rows; LEFT and RIGHT keep all 8,124 |
| 14 | **4.4 SEMI / ANTI** | Anti join returns only edible mushrooms; 0 undocumented codes |
| 15 | **4.5 FULL OUTER** | Spore colors with 0 edible or 0 poisonous still appear |
| 16 | **4.6 CROSS JOIN** | Grid of 63 combinations, some with 0 mushrooms |
| 17 | **Part 5: Spark SQL** (Q1 → Q4) | Q4: odor rule accuracy = **98.52%** |
| 18 | **6.1 → 6.3** Features, split, evaluation function | Train ≈ 80% / Test ≈ 20% |
| 19 | **6.4 Logistic Regression** and **6.5 Random Forest** | Accuracy ≈ 1.0, AUC 1.0, 0 false negatives |
| 20 | **6.6 Feature importance** | `odor` is the most important feature |
| 21 | **6.7 Without odor / spore color** | Accuracy still very high |
| 22 | **6.8 Cross-validation** and **6.9 Comparison** | Comparison table of all models |
| 23 | **Overfitting checks** | Train ≈ test accuracy, stable across 5 seeds → no overfitting |
| 24 | Last cell: `spark.stop()` | Spark stops |

---

### Workshop 2: Car Evaluation, step by step

| Step | What to do | What you should see |
|---|---|---|
| 1 | Check that `data/car_data.csv` exists (download it from the [GitHub link](https://github.com/muttinenisairohith/Datasets/blob/6e7bec3b9cbdc25da1055472c837ef8a10f569ed/data/car_data.csv) with **Download raw file** if not) | File in `data/` |
| 2 | Open `second_workshop.ipynb` and run the first cell (SparkSession + loading) | `Rows: 1728`, 7 string columns |
| 3 | **Part 2: Target distribution** | unacc 70.0%, acc 22.2%, good 4.0%, vgood 3.8%: **imbalanced** |
| 4 | **Missing values + crosstabs** | No missing values; `safety = low` and `persons = 2` → always `unacc` |
| 5 | **Part 3: StringIndexer + VectorAssembler** | `{0: 'unacc', 1: 'acc', 2: 'good', 3: 'vgood'}` |
| 6 | **Train/test split** | Train 1,431 / Test 297 |
| 7 | **Part 4: Evaluation function** | (defines `evaluate`, no output) |
| 8 | **5.1 Logistic Regression** | Accuracy ≈ 0.65, recall good = 0 |
| 9 | **5.2 Decision Tree (depth 3)** | Accuracy ≈ 0.80, but good / vgood never predicted |
| 10 | **5.3 Random Forest** | Accuracy ≈ 0.97, all classes detected |
| 11 | **Feature importance** | safety and persons on top |
| 12 | **Part 6: Logistic Regression + OneHot** | Accuracy ≈ 0.95 (vs 0.65 without one-hot) |
| 13 | **Part 7: Comparison table** and `spark.stop()` | Random Forest is the best model |

---

### Workshop 3: Streaming pipeline, step by step

| Step | What to do | What you should see |
|---|---|---|
| 1 | Start Docker Desktop, then: `cd ~/Documents/PySpark/data_engineering_project && docker compose up -d` | Images download, containers start |
| 2 | Wait 2 minutes, then `docker compose ps` | 8 containers **Up** |
| 3 | Open Kafka UI: http://localhost:8888 | Cluster `local` with **3 brokers** |
| 4 | Open Airflow: http://localhost:8080 and log in (`admin` + password from `docker exec -it airflow cat /opt/airflow/standalone_admin_password.txt`) | DAG `random_people_names` in the list |
| 5 | Kafka UI → **Topics → Add a Topic**: `random_names`, 1 partition, replication factor 3 | Topic created, 0 out-of-sync replicas |
| 6 | `docker exec -it cassandra cqlsh -f /scripts/create_tables.cql` | Keyspace `spark_streaming` and table `random_names` created |
| 7 | In Airflow, switch the DAG **on** and click **▶ Trigger DAG** | Task runs for 2 min, then turns dark green |
| 8 | Kafka UI → **Topics → random_names → Messages** | JSON messages with random people (12 per run) |
| 9 | Run the **spark-submit** command (see [Step 5](#step-5-start-the-spark-streaming-job) below) and leave the terminal open | `Streaming is being started...` |
| 10 | In a second terminal: `docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM spark_streaming.random_names;"` | Rows from Kafka are in Cassandra |
| 11 | Trigger the DAG again and re-run the COUNT every 20 s | The count increases every 10 s: **real-time streaming** ✅ |
| 12 | Stop: `Ctrl + C` in the Spark terminal, then `docker compose stop` | Everything stopped |

Detailed explanations, commands and troubleshooting for workshop 3 are in the [Workshop 3 section](#workshop-3-data-engineering-end-to-end-streaming-pipeline) below.

---

## Workshop 1: Mushroom Classification

**Goal:** predict whether a mushroom is **edible** or **poisonous** from 22 physical characteristics, and practise every type of Spark join along the way.

**Data:** 8,124 mushrooms, 22 categorical features coded as single letters, target `class` (`e` = edible, `p` = poisonous). It is downloaded automatically with `kagglehub` in the first cell.

### Steps

| Part | Content |
|---|---|
| 1. Loading | SparkSession, read CSV as strings, rename `cap-shape` → `cap_shape` |
| 2. Exploration | Class balance, distinct values per column, missing values (`?`), duplicates, `crosstab` odor × class |
| 3. Cleaning | Drop constant column `veil_type`; keep `?` in `stalk_root` as its own category (30.5% of rows) |
| 4. Joins | Lookup table (code → label) used to practise every join type (see below) |
| 5. Spark SQL | Temp views, % poisonous by odor, spore color for odorless mushrooms, `LEFT ANTI JOIN` in SQL |
| 6. Machine learning | `StringIndexer` → `OneHotEncoder` → `VectorAssembler` → Logistic Regression / Random Forest, cross-validation, feature importance |
| 7. Validation | Train vs. test accuracy, several random splits (overfitting check) |

### Joins practised

| Join | Use in the workshop |
|---|---|
| **LEFT** | Decode letter codes into labels without losing any mushroom |
| **INNER** | Shows rows lost when the lookup is incomplete |
| **RIGHT** | Same as LEFT with the tables swapped |
| **LEFT SEMI** | Mushrooms whose odor was seen on at least one poisonous mushroom |
| **LEFT ANTI** | Mushrooms with "safe" odors, plus a data-quality check for undocumented codes |
| **FULL OUTER** | Edible vs. poisonous counts per spore color, keeping colors found in only one class |
| **CROSS** | Full odor × habitat grid (63 combinations), including combinations that never occur |

### Key results

- Balanced classes (≈52% edible / 48% poisonous), no duplicates.
- **Odor** is by far the strongest feature: 8 of 9 odor values belong to a single class. The rule "poisonous unless odor is almond, anise or none" gives **98.52%** accuracy (only 120 errors, all odorless poisonous mushrooms).
- Logistic Regression and Random Forest reach **≈100% accuracy, AUC 1.0, recall (poisonous) 1.0** on the test set.
- **Not overfitting:** train and test accuracy are identical and stable across random splits. The dataset is almost perfectly separable.
- Limitation: only 23 species from a field guide. Never use such a model to decide whether a real mushroom is safe to eat.

---

## Workshop 2: Car Evaluation (multiclass classification)

**Goal:** predict a car's acceptability (`unacc`, `acc`, `good`, `vgood`) from 6 categorical features, following [Pyspark MLlib | Classification using Pyspark ML](https://towardsai.com/p/l/pyspark-mllib-classification-using-pyspark-ml).

**Data:** `data/car_data.csv`, 1,728 cars, no missing values.

| Column | Values |
|---|---|
| buying, maintenance | vhigh, high, med, low |
| doors | 2, 3, 4, 5more |
| persons | 2, 4, more |
| lug_boot | small, med, big |
| safety | low, med, high |
| **car_type** (target) | unacc 70.0%, acc 22.2%, good 4.0%, vgood 3.8% |

> The CSV header spells the column `maintainence`; the notebook renames it to `maintenance`.

### Steps

1. Load the data and explore the target distribution (**imbalanced**: a model always answering "unacc" already gets 70%).
2. Crosstabs: `safety = low` or `persons = 2` always means `unacc`.
3. `StringIndexer` + `VectorAssembler` in a Pipeline, 80/20 split (`seed=42`).
4. Evaluation with `MulticlassClassificationEvaluator`: accuracy, weighted F1, **recall per class**, confusion matrix.
5. Models: Logistic Regression, Decision Tree (depth 3), Random Forest (500 trees, depth 10), plus Logistic Regression with **OneHotEncoder**.

### Results

| Model | Accuracy | Weighted F1 | Recall good | Recall vgood |
|---|---|---|---|---|
| Logistic Regression | 0.650 | 0.615 | 0.00 | 0.09 |
| Decision Tree (depth 3) | 0.805 | 0.765 | 0.00 | 0.00 |
| **Random Forest** | **0.970** | **0.970** | 0.89 | 0.91 |
| Logistic Regression + OneHot | 0.946 | 0.947 | 1.00 | 0.86 |

**Takeaways**
- Accuracy alone is misleading on imbalanced data: the Decision Tree reaches 80% but never predicts `good` or `vgood`.
- Logistic Regression fails when categories are fed as plain numbers (0, 1, 2…). **One-hot encoding** raises it from 0.65 to 0.95.
- Random Forest is the best model. The most important features are **safety** (0.30) and **persons** (0.24).
- Fix compared to the article: the article labels its metric "Area Under ROC", but `MulticlassClassificationEvaluator` computes F1 by default, and AUC does not apply to 4 classes.

---

## Workshop 3: Data Engineering End-to-End Streaming Pipeline

All files for this workshop are in `data_engineering_project/` (the same guide is also in [`data_engineering_project/README.md`](data_engineering_project/README.md)).

A streaming pipeline that fetches random user data from an API, sends it through **Kafka**, processes it with **Spark Structured Streaming**, and stores it in **Cassandra**. **Airflow** orchestrates the producer, and everything runs in **Docker**.

Based on the article [Data Engineering End-to-End Project — 1](https://medium.com/@dogukannulu/data-engineering-end-to-end-project-1-7a7be2a3671) by Dogukan Ulu, with updated Docker images and bug fixes.

---

### Architecture

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

### Tech stack

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

### Project structure

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

#### Data model (Kafka message = Cassandra row)

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

### Prerequisites

- **Docker Desktop** running, with at least **6 GB of memory** allocated (Settings → Resources).
- Internet access (Docker images, Random User API, Spark connector download).
- Tested on macOS with Apple Silicon. All images also support Intel/amd64.

---

### How to run

#### Step 1: Start all services

```bash
cd ~/Documents/PySpark/data_engineering_project
docker compose up -d
docker compose ps        # 8 containers should be "Up"
```

Wait about 2 minutes for Cassandra and Airflow to start.

- **Kafka UI**: http://localhost:8888 (cluster `local` with 3 brokers)
- **Airflow**: http://localhost:8080. User `admin`. If `admin` / `admin` does not work, get the generated password:
  ```bash
  docker exec -it airflow cat /opt/airflow/standalone_admin_password.txt
  ```

#### Step 2: Create the Kafka topic

In Kafka UI: **Topics → Add a Topic**. Name `random_names`, partitions `1`, replication factor `3`.

Or from the terminal:

```bash
docker exec -it kafka1 kafka-topics --create --topic random_names \
  --partitions 1 --replication-factor 3 --bootstrap-server kafka1:19092

docker exec -it kafka1 kafka-topics --describe --topic random_names --bootstrap-server kafka1:19092
```

#### Step 3: Create the Cassandra keyspace and table

```bash
docker exec -it cassandra cqlsh -f /scripts/create_tables.cql
docker exec -it cassandra cqlsh -e "DESCRIBE TABLE spark_streaming.random_names;"
```

(If you get `Connection refused`, Cassandra is still starting. Wait 30 seconds.)

#### Step 4: Run the producer with Airflow

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

#### Step 5: Start the Spark streaming job

```bash
docker exec -it spark_master /opt/spark/bin/spark-submit \
  --master "local[2]" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  /opt/spark-apps/spark_streaming.py
```

The job keeps running and waits for new messages. Leave this terminal open, and stop it with `Ctrl + C`.

#### Step 6: Check the data in Cassandra

In a second terminal:

```bash
docker exec -it cassandra cqlsh -e "SELECT full_name, country, city, email FROM spark_streaming.random_names;"
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM spark_streaming.random_names;"
```

Trigger the DAG again: the count increases by 1 every 10 seconds (real-time streaming end to end).

---

### Stop and clean up

```bash
docker compose stop          # stop containers (data kept)
docker compose down          # remove containers (Cassandra data kept in the volume)
docker compose down -v       # remove containers AND Cassandra data
```

---

### Changes compared to the original article

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

### Troubleshooting

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

### Key concepts

- **Kafka topic**: a named stream of messages (`random_names`). Producers write to it, consumers read from it.
- **Broker**: a Kafka server. With 3 brokers and **replication factor 3**, every message is copied on all brokers.
- **ZooKeeper**: coordinates the Kafka cluster (live brokers, controller election, partition leaders, metadata). It does not store the messages.
- **DAG** (Airflow): a workflow of tasks with a schedule. Here: one task, `kafka_data_stream`, daily at 01:00.
- **Structured Streaming**: Spark treats the Kafka topic as an infinite table, and each new message becomes a new row processed in micro-batches.
- **Checkpoint**: Spark saves which Kafka offsets were already processed, so a restart continues where it stopped.
- **Keyspace** (Cassandra): equivalent of a database in SQL.

---

## Warm-up: California Housing (`testing.ipynb`)

Linear regression (ElasticNet) on the California Housing dataset: schema definition, exploration, feature engineering (rooms per household, population per household, bedrooms per room), `VectorAssembler`, `StandardScaler`, and model evaluation.

---

## Concepts covered

- **Spark basics:** SparkSession, DataFrames, lazy evaluation, `cache()`, `groupBy`, `agg`, `crosstab`, `pivot`, `unpivot`
- **Joins:** inner, left, right, full outer, left semi, left anti, cross, broadcast joins
- **Spark SQL:** temp views, `CASE WHEN`, aggregations, `LEFT ANTI JOIN` syntax
- **Spark MLlib:** Pipelines, `StringIndexer`, `OneHotEncoder`, `VectorAssembler`, `StandardScaler`, Logistic/Linear Regression, Decision Tree, Random Forest, `CrossValidator`
- **Evaluation:** accuracy, F1, AUC, recall per class, confusion matrix, overfitting checks, imbalanced classes
- **Data engineering:** Kafka topics, brokers, replication, ZooKeeper, Airflow DAGs, Spark Structured Streaming, checkpoints, Cassandra keyspaces, Docker Compose
