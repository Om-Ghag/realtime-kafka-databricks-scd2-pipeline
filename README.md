# realtime-kafka-databricks-scd2-pipeline
Built an end-to-end real-time data pipeline (Kafka → Databricks) with Medallion Architecture and SCD Type 2, enabling scalable and historical data tracking using Delta Lake.

# 🚀 Real-Time Data Pipeline using Kafka, Databricks & Delta Lake (SCD Type 2)

---

## 📌 Project Overview

This project demonstrates an end-to-end **real-time data engineering pipeline** using **Confluent Kafka (Cloud)** and **Databricks Structured Streaming**.

The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold)** and implements **Slowly Changing Dimension (SCD Type 2)** to track historical changes in transactional data.

---

## 🧱 Architecture

```
Kafka Producer
     ↓
Confluent Kafka Topic (ecommerce_events)
     ↓
Bronze Layer (Raw Streaming Ingestion)
     ↓
Silver Layer (Schema Enforcement & Cleansing)
     ↓
Gold Layer (SCD Type 2 Implementation)
     ↓
Delta Lake Tables (Analytics Ready)
```

---

## ⚙️ Tech Stack

* Apache Kafka (Confluent Cloud)
* Databricks (Serverless)
* PySpark (Structured Streaming)
* Delta Lake
* Python

---

## 📦 Data Model

Sample event:

```json
{
  "order_id": 101,
  "user_id": "user_101",
  "amount": 250
}
```

---

## 🪙 Bronze Layer (Raw Ingestion)

### Purpose:

* Ingest real-time data from Kafka
* Store raw JSON with metadata

### Key Features:

* Kafka integration using SASL_SSL authentication
* Captures metadata:

  * topic
  * partition
  * offset
  * kafka_timestamp
* Adds ingestion timestamp

### Output Table:

```
workspace.streaming.ecommerce_bronze
```

---

## 🪙 Silver Layer (Transformation)

### Purpose:

* Parse JSON data
* Clean and structure records

### Key Features:

* Schema enforcement using `from_json`
* Filters invalid records
* Adds processing timestamp
* Deduplication using event time

### Output Table:

```
workspace.streaming.ecommerce_silver
```

---

## 🪙 Gold Layer (SCD Type 2)

### Purpose:

* Maintain historical changes in data

### Logic:

1. Deduplicate records using latest event timestamp
2. Identify changed records
3. Expire old records (`is_current = false`)
4. Insert new records

### Key Features:

* Uses `foreachBatch` for batch processing
* Delta Lake MERGE logic
* Maintains:

  * `start_date`
  * `end_date`
  * `is_current`

### Output Table:

```
workspace.streaming.ecommerce_gold_scd2
```

---

## 🔄 Data Flow

1. Producer sends JSON data to Kafka topic
2. Bronze layer ingests raw data
3. Silver layer cleans and structures data
4. Gold layer applies SCD Type 2 logic

---

## ⚡ Performance Testing

| Records | Time Taken |
| ------- | ---------- |
| 10k     | ~4.5 min   |
| 50k     | ~8.5 min   |
| 100k    | ~15 min    |

### Observations:

* Pipeline scales efficiently (sub-linear growth)
* Kafka ingestion (Bronze) is the main bottleneck
* Transformations (Silver/Gold) are optimized

---

## 🔧 Optimizations Implemented

* Kafka partitioning for parallel ingestion
* Key-based partitioning in producer
* Checkpoint management for fault tolerance
* Deduplication using window functions
* Efficient SCD Type 2 logic

---

## ⚠️ Challenges Faced

### 1. Kafka Checkpoint Issue

* Error due to topic recreation
* Fixed by resetting checkpoints

### 2. Delta Table Error

* MERGE failed because table didn’t exist
* Fixed by pre-creating Delta table

### 3. Kafka Partition Bottleneck

* Single partition caused slow ingestion
* Fixed by increasing partitions and using keys

---

## 💰 Cost Optimization

* Used Confluent free tier (1 eCKU free)
* Used Databricks serverless (auto-scaling)
* Used `availableNow=True` to avoid continuous costs

---

## 🚀 How to Run

### 1. Start Kafka Producer

```bash
python producer/kafka_producer.py
```

---

### 2. Run Databricks Notebooks

Run in order:

1. Bronze
2. Silver
3. Gold

---

### 3. Ensure Gold Table Exists

```sql
CREATE TABLE workspace.streaming.ecommerce_gold_scd2 (...)
USING DELTA;
```

---

## 🧠 Key Learnings

* Built real-time streaming pipeline using Kafka and Spark
* Understood micro-batch processing in Structured Streaming
* Implemented SCD Type 2 using Delta Lake
* Learned importance of Kafka partitioning
* Handled checkpoint and state management

---

## 🚀 Future Enhancements

* Add CDC (Insert/Update/Delete handling)
* Implement late data handling using watermarking
* Add monitoring and alerting
* Optimize Kafka ingestion further
* Add dashboard using Power BI

---

## 👨‍💻 Author

This project was built as a hands-on implementation of real-time data engineering concepts using modern cloud tools.
