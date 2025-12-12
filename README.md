

# 🏀 Bluesky NBA Real-Time Sentiment Analysis

A real-time data streaming pipeline that captures live posts from **Bluesky** regarding the **NBA**, performs sentiment analysis using **Apache Spark Structured Streaming**, and visualizes the results in **Grafana** via **InfluxDB**.

## 📖 Background
While watching the NBA playoffs and scrolling through social media, I wondered if I could gauge general fan sentiment in real-time. Originally designed for Twitter, this project now leverages the **Bluesky Jetstream** API to fetch live posts without rate-limit headaches. The project focuses on building a robust ETL pipeline using Kafka, Spark, and time-series databases.

## 🏗 Architecture

The pipeline consists of four main stages:

![plot](docs/architecture.jpg)

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
*   **Database:** InfluxDB (Time Series)
*   **Visualization:** Grafana
*   **Libraries:** `websockets`, `kafka-python`, `textblob`, `influxdb-client`

### Prerequisites
*   Java (JDK 8 or 11) is required for Kafka and Spark.
*   Docker (optional, but recommended for InfluxDB/Grafana).

---

## ⚙️ Installation & Setup

### 1. Python Environment
Create a virtual environment and install the dependencies:

```bash
pip install pyspark kafka-python-ng websockets textblob influxdb-client python-dotenv
```

### 2. Configuration (`.env`)
Create a `.env` file in the root directory. This keeps your credentials safe.

```ini
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=twitterdata
KAFKA_OUTPUT_TOPIC=twitterdata-clean
KAFKA_GROUP_ID=bluesky-influx-group

# Spark
SPARK_APP_NAME=BlueskySentimentAnalysis
SPARK_CHECKPOINT_DIR=/tmp/spark-checkpoint

# InfluxDB
INFLUXDB_URL=http://localhost:8086
influxdb_token=YOUR_INFLUX_TOKEN_HERE
influxdb_org=YOUR_ORG_NAME
influxdb_bucket=YOUR_BUCKET_NAME
```

### 3. Start Infrastructure
You need to start Zookeeper, Kafka, and InfluxDB.

**Option A: Using Terminal (Local Install)**
```bash
# 1. Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# 2. Start Kafka
bin/kafka-server-start.sh config/server.properties

# 3. Create Topics
bin/kafka-topics.sh --create --topic twitterdata --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic twitterdata-clean --bootstrap-server localhost:9092
```

**Option B: Start InfluxDB & Grafana**
```bash
# Mac (Homebrew)
brew services start influxdb
brew services start grafana-server
```

---

## 🚀 How to Run

Run the scripts in the following order using separate terminal tabs:

**Step 1: Start the Producer (Ingestion)**
Connects to Bluesky and sends data to Kafka.
```bash
python extract_bluesky_to_kafka.py
```

**Step 2: Start Spark Streaming (Processing)**
Reads from Kafka, applies sentiment analysis, and writes back to Kafka.
*Note: The script automatically handles the `spark-sql-kafka` package.*
```bash
python process_spark_streaming.py
```

**Step 3: Start the Loader (Storage)**
Reads from the processed Kafka topic and saves to InfluxDB.
```bash
python load_kafka_to_influxdb.py
```

**Step 4: Visualize**
1.  Open Grafana (`http://localhost:3000`).
2.  Add InfluxDB as a Data Source.
3.  Create a dashboard to query the measurement `bluesky_post`.

---

## 🧠 Technical Concepts (The "Why")

This project was built to explore specific engineering concepts:

### ⚡ Why is Kafka so fast?
Kafka is optimized for high throughput and low latency I/O.
1.  **Sequential I/O:** Kafka avoids random disk access (which is slow). It treats data as a "Log" (append-only structure). Writing to the end of a file sequentially is nearly as fast as writing to RAM.
2.  **Zero Copy Principle:**
    *   *Traditional:* Data is copied from Disk → Kernel → Application → Kernel → Network (4 copies).
    *   *Kafka:* Uses the `sendfile` system call to transfer data directly from Disk Cache to Network Socket, bypassing the application context entirely. This reduces CPU cycles and memory usage.

### ⚡ 🏗 Kafka Internal Architecture
![plot](docs/architecture.jpg)

### 🌊 Handling Late Data (Watermarking)
In streaming, "Event Time" (when the post was written) differs from "Processing Time" (when Spark receives it).
*   **The Problem:** What if a post from 12:00 arrives at 12:05 due to network lag?
*   **The Solution:** Spark uses **Watermarking**.
    *   `df.withWatermark("ts", "10 seconds")` tells the engine: "Wait for late data up to 10 seconds. Anything older than that is dropped."
    *   This prevents the application from keeping infinite state in memory waiting for old data.

### 📊 Structured Streaming Modes
*   **Append Mode:** Only new rows are added to the result table. (Used in this project for writing to Kafka).
*   **Complete Mode:** The entire result table is rewritten every trigger. (Useful for aggregations/counts).
*   **Update Mode:** Only rows that changed are written.

---

## 🔮 Future Enhancements
*   **Dockerize:** Create a `docker-compose.yml` to launch Kafka, Spark, InfluxDB, and the Python scripts with one command.
*   **Better NLP:** Replace `TextBlob` with a pre-trained Transformer model (like BERT) for higher accuracy on slang/sports terms.
*   **Direct Sink:** Write from Spark directly to InfluxDB (skipping the second Kafka topic) to reduce latency.

## 📝 Credits
Inspiration and references taken from various Spark Streaming tutorials and the official documentation for Apache Kafka and InfluxDB.
