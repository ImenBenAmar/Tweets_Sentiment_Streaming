

# 🦋 Bluesky NBA Real-Time Sentiment Analysis

A real-time data streaming pipeline that captures live posts from **Bluesky** regarding the **NBA**, performs sentiment analysis using **Apache Spark Structured Streaming**, and visualizes the results in **Grafana** via **InfluxDB**.

## 📖 Background
While watching the NBA playoffs and scrolling through social media, I wondered if I could gauge general fan sentiment in real-time. Originally designed for Twitter, this project now leverages the **Bluesky Jetstream** API to fetch live posts without rate-limit headaches. The project focuses on building a robust ETL pipeline using Kafka, Spark, and time-series databases.

## 🏗 Architecture

The pipeline consists of four main stages:

![plot](docs/architecturea.png)

1.  **Ingestion:** Python script connects to Bluesky Jetstream (WebSocket) and filters for "NBA" posts in English.
2.  **Buffering:** Raw JSON data is pushed to Apache Kafka (Topic: `twitterdata`).
3.  **Processing:** PySpark reads the stream, cleans text, and calculates sentiment (Polarity/Subjectivity) using TextBlob.
4.  **Storage & Viz:** Processed data is sent back to Kafka, consumed by a loader script, stored in InfluxDB, and visualized in Grafana.

---

## 🛠 Technologies & Prerequisites

### Tools
*   **Language:** Python 3.8+
*   **Streaming Platform:** Apache Kafka & Zookeeper
*   **Processing Engine:** Apache Spark (PySpark 3.5.x)
*   **Database:** InfluxDB (Time Series) & Elasticsearch (NoSQL/Search)
*   **Visualization:** Grafana & Kibana
*   **Libraries:** `websockets`, `kafka-python`, `textblob`, `influxdb-client`

### Prerequisites
*   Java (JDK 8 or 11) is required for Kafka and Spark.
*   Docker (optional, but recommended for InfluxDB/Grafana).

---

## ⚙️ Installation & Setup

### 1. Python Environment
Create a virtual environment and install the dependencies:

```bash
pip install pyspark textblob python-dotenv
pip install kafka-python influxdb-client elasticsearch python-dotenv
```

### 2. Configuration (`.env`)
Create a `.env` file in the root directory. This keeps your credentials safe.

```ini
# --- KAFKA ---
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=twitterdata

