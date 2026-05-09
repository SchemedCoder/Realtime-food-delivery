# Real-Time Food Delivery Streaming Pipeline 🍔

## Overview
Built a real-time streaming pipeline using Kafka, Docker, and PySpark to process food delivery orders and monitor delivery SLA metrics.

---

## Architecture
Producer → Kafka → Spark Streaming → Real-time Aggregation

---

## Features
- Real-time order ingestion
- SLA breach detection
- Aggregation by city
- Dockerized Kafka setup
- Data quality handling

---

## Tech Stack
- Docker
- Apache Kafka
- PySpark
- Python

---

## Business Insights
- Average delivery time
- Delayed order tracking
- Average order value
- City-level analytics

---

## Run Project

### Start Kafka
```bash
docker-compose up -d
```

### Run Producer
```bash
python producer/producer.py
```

### Run Spark Job
```bash
spark-submit spark/streaming_job.py
```

---

