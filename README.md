# E-commerce Streaming Data Processing

A real-time data engineering project that streams e-commerce user events from **Apache Kafka (Confluent Cloud)** into **Databricks** and processes them through a **Medallion Architecture** (Bronze → Silver → Gold) using Databricks Declarative Pipelines (Lakeflow).

---

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Event Schema](#event-schema)
- [Pipeline Layers](#pipeline-layers)
- [Project Structure](#project-structure)
- [Setup & Installation](#setup--installation)
- [Running the Pipeline](#running-the-pipeline)
- [Sample Results](#sample-results)
- [Data Quality](#data-quality)
- [Key Learnings](#key-learnings)

---

## Overview

This project simulates an e-commerce platform that emits user activity events (page views, add-to-cart, orders, payments) to a Kafka topic. A Databricks pipeline continuously ingests these events, cleans and validates them, and builds an analytics-ready star schema (dimension and fact tables).

**Goals**
- Ingest streaming data from Kafka into Databricks
- Apply the Bronze / Silver / Gold medallion pattern
- Enforce data quality with pipeline expectations
- Model the data as a star schema for analytics

---

## Architecture

```
┌────────────────────┐     ┌──────────────────────┐     ┌──────────────────┐     ┌─────────────────────────┐
│  Event Producer    │     │  Confluent Cloud     │     │  BRONZE          │     │  SILVER                 │
│  (e-commerce       │ ──► │  Kafka               │ ──► │  bronze_kafka_   │ ──► │  silver_events          │
│   events)          │     │  topic_kafka_learning│     │  events          │     │  (cleaned + validated)  │
└────────────────────┘     └──────────────────────┘     └──────────────────┘     └───────────┬─────────────┘
                                                                                             │
                                          ┌──────────────┬───────────────┬───────────────┬───┴────────────┐
                                          ▼              ▼               ▼               ▼                ▼
                                    dim_customers  dim_event_types  dim_products     dim_time     fact_engagement
                                                         GOLD (Materialized Views)
```

| Layer  | Object type        | Tables |
|--------|--------------------|--------|
| Source | Kafka topic        | `topic_kafka_learning` |
| Bronze | Streaming table    | `bronze_kafka_events` |
| Silver | Streaming table    | `silver_events` |
| Gold   | Materialized views | `dim_customers`, `dim_event_types`, `dim_products`, `dim_time`, `fact_engagement` |

---

## Tech Stack

- **Streaming platform:** Apache Kafka on Confluent Cloud (6 partitions, SASL/PLAIN authentication)
- **Processing:** Databricks (Free Edition), Lakeflow Declarative Pipelines
- **Language:** Python (PySpark) with Spark Structured Streaming
- **Storage / governance:** Unity Catalog (catalog `e-commerce`, schema `default`)
- **Architecture pattern:** Medallion (Bronze / Silver / Gold), star schema

---

## Event Schema

Each Kafka message uses the user ID as the **key** (e.g. `U1`, `U30`, `U47`) and a **JSON value**:

```json
{
  "event_id": "f1ced48e-68e9-449d-9bb5-fbbb1eb3b097",
  "event_type": "page_view",
  "event_time": "2026-10-13T00:49:27Z",
  "...": "additional fields (user, product, etc.)"
}
```

**Event types:** `page_view`, `add_to_cart`, `order_placed`, `payment`

---

## Pipeline Layers

### 🥉 Bronze: `bronze_kafka_events`
Raw ingestion from Kafka using Structured Streaming. Key and value are cast to strings and Kafka metadata is preserved.

```python
spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "<bootstrap-server>")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", "<jaas-config>")
    .option("subscribe", "topic_kafka_learning")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr(
        "CAST(key AS STRING) AS key",
        "CAST(value AS STRING) AS value",
        "topic", "partition", "offset",
        "timestamp AS kafka_timestamp",
        "timestampType",
        "current_timestamp() AS _ingested_at"
    )
```

### 🥈 Silver: `silver_events`
Parses the JSON payload into typed columns, deduplicates, and applies **4 data quality expectations**.

### 🥇 Gold: Star schema (Materialized Views)

| Table             | Type      | Description |
|-------------------|-----------|-------------|
| `dim_customers`   | Dimension | Unique customers derived from events |
| `dim_event_types` | Dimension | Event type lookup (page_view, add_to_cart, order_placed, payment) |
| `dim_products`    | Dimension | Products referenced in events |
| `dim_time`        | Dimension | Time attributes for event timestamps |
| `fact_engagement` | Fact      | User engagement events linked to all dimensions |

---

## Project Structure

```
transformations/
├── bronze/
│   └── bronze_kafka.py        # Kafka → bronze_kafka_events
├── silver/
│   └── silver_events.py       # Clean + validate → silver_events
├── gold/
│   ├── dim_customers.py
│   ├── dim_event_types.py
│   ├── dim_products.py
│   ├── dim_time.py
│   └── fact_engagement.py
└── my_transformation.py
```

---

## Setup & Installation

### Prerequisites
- A [Confluent Cloud](https://www.confluent.io/) account with a Kafka cluster
- A [Databricks](https://www.databricks.com/) workspace (Free Edition works)

### 1. Create the Kafka topic
1. In Confluent Cloud, create a cluster (e.g. `Learning`).
2. Create the topic `topic_kafka_learning` (6 partitions).
3. Create an API key and secret for the cluster.

### 2. Store credentials securely
Don't hardcode credentials in the code. Store them with Databricks secrets (or widgets / pipeline configuration on the Free Edition):

```python
username = dbutils.secrets.get(scope="kafka", key="api_key")
password = dbutils.secrets.get(scope="kafka", key="api_secret")
```

### 3. Produce events
Run your producer script to publish events to `topic_kafka_learning`.

```bash
python producer.py
```

### 4. Create the Databricks pipeline
1. Go to **Jobs & Pipelines → Create → ETL pipeline**.
2. Set catalog to `e-commerce` and schema to `default`.
3. Add the files from `transformations/` to the pipeline.
4. Click **Run pipeline**.

---

## Running the Pipeline

| Action | Description |
|--------|-------------|
| **Dry run** | Validates the pipeline without processing data |
| **Run pipeline** | Executes a refresh of all tables |
| **Run file** | Runs a single transformation file |

Monitor runs in **Pipeline graph**, **Tables**, and **Performance** tabs, or open the **event log** for details.

---

## Sample Results

**Initial run (full load)**

| Table | Output records | Incrementalization |
|-------|---------------:|--------------------|
| bronze_kafka_events | 15 | N/A |
| silver_events | 13 | N/A |
| dim_customers | 12 | Full recompute |
| dim_event_types | 3 | Full recompute |
| dim_products | 5 | Full recompute |
| dim_time | 1 | Full recompute |
| fact_engagement | 13 | Full recompute |

**Subsequent run (after more events were produced)**

| Table | Output records | Incrementalization |
|-------|---------------:|--------------------|
| dim_customers | 23 | No change* |
| dim_event_types | 3 | No change* |
| dim_products | 6 | No change* |
| dim_time | 2 | No change* |
| fact_engagement | 24 | No change* |

Total run duration: ~34 seconds, status **Completed**.

---

## Data Quality

`silver_events` defines 4 expectations. In the first run, **3 were met and 1 was unmet**: 15 bronze records produced 13 silver records, so 2 records were flagged or dropped. Review the expectation details in the pipeline's **Tables** tab → *Expectations* column.

---

## Key Learnings

- Connecting Databricks to Confluent Cloud Kafka with SASL_SSL
- Streaming tables vs. materialized views in declarative pipelines
- Medallion architecture and star schema modeling
- Declarative data quality checks with expectations
- Full recompute vs. incremental refresh behavior

---
