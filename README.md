# Smart City End-to-End Real-Time Data Streaming Pipeline 🚦🏙️

[![Executive Showcase Deck](https://img.shields.io/badge/%F0%9F%8E%AF_Interactive_Showcase_Deck-Recruiters_%26_Hiring_Managers-FF9900?style=for-the-badge)](https://siza94.github.io/Smart-City-AWS-Streaming-Data-Engineering-Project/)
[![8-Phase Engineering Report](https://img.shields.io/badge/%F0%9F%93%98_8--Phase_Engineering_Report-Architecture_%26_Lifecycle-0284C7?style=for-the-badge)](https://siza94.github.io/Smart-City-AWS-Streaming-Data-Engineering-Project/smart_city_project_report.html)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Sithsaba_Zantsi-0A66C2?style=for-the-badge&logo=linkedin)](https://www.linkedin.com/in/sithsaba-zantsi/)

> ### 👋 Visiting from a CV or Application? Start with the Interactive Artifacts:
> | Artifact | Audience | What You'll See | Direct Link |
> | :--- | :--- | :--- | :--- |
> | **🎯 Executive & Recruiter Showcase Deck** | Recruiters, Hiring Managers, Business Owners | 3-persona lens switcher (`Recruiter` / `Lead DE` / `Executive`), clickable AWS + Kafka architecture explorer, and a live London–Birmingham telemetry simulator. | **[Launch Showcase Deck ↗](https://siza94.github.io/Smart-City-AWS-Streaming-Data-Engineering-Project/)** |
> | **📘 8-Phase Engineering Lifecycle Report** | Senior Data Engineers & Technical Interviewers | Full lifecycle record (Phases 01–08): UK INRIX congestion evidence, architecture trade-offs, star-schema ERD, and production streaming implementation. | **[Read Engineering Report ↗](https://siza94.github.io/Smart-City-AWS-Streaming-Data-Engineering-Project/smart_city_project_report.html)** |

---

This project implements a full real-time data engineering pipeline simulating a connected urban corridor between **London (`51.5074°N, -0.1278°W`)** and **Birmingham (`52.4862°N, -1.8904°W`)**. By integrating multi-sensor IoT telemetry, containerized stream processing, and managed AWS cloud analytics, the solution enables sub-minute monitoring of traffic flow, weather conditions, GPS trajectories, and emergency incidents.

---

## 🚀 Key Architectural Highlights

1. **Co-Partitioned Kafka Ordering (`jobs/main.py`)**: Keys Kafka messages by `deviceId` (`Vehicle-123`) so all 5 telemetry streams for a vehicle land in strict chronological order on the same partition.
2. **Typed Geospatial Schema Contract (`jobs/spark-city.py`)**: Enforces explicit `latitude: DoubleType` and `longitude: DoubleType` fields across all 5 sensor schemas to prevent `NULL` coercion on read.
3. **Watermarked Silver Stream-Stream Join (`jobs/spark-city.py`)**: Executes a watermarked (`2 minutes`) stream-stream join (`corridorSilverDF`) across `vehicle_data`, `weather_data`, and `emergency_data` on `(deviceId, timestamp ± 1 min)` into `s3a://.../data/silver_corridor_telemetry`.
4. **S3 Small-File Mitigation & Secret Hygiene (`jobs/spark-city.py`, `jobs/config.py`)**: Uses `.trigger(processingTime='60 seconds')` micro-batching to reduce S3 Parquet object proliferation by 20x and loads AWS credentials securely via environment variables (`os.getenv`).

---

## ⚙️ Key Features

- **Real-time data ingestion** from 5 synchronized IoT sensor streams (`vehicle_data`, `gps_data`, `traffic_data`, `weather_data`, `emergency_data`)
- **Kafka-based message streaming** with Confluent Server 7.4.0 & ZooKeeper orchestrated via Docker Compose
- **Spark 3.5 Structured Streaming** with 2-minute event-time watermarking, S3A fault-tolerant checkpointing, and stream-stream joins
- **AWS S3, Glue, Redshift, Athena, and QuickSight** for cloud lakehouse storage, cataloging, serverless SQL, and operational BI
- **Interactive HTML Portfolio Artifacts** documenting the full 8-phase engineering lifecycle and executive value

---

## 🔧 Technologies Used

| Category            | Tools / Services                                 |
|---------------------|--------------------------------------------------|
| Ingestion           | Apache Kafka (`cp-server:7.4.0`), ZooKeeper, Docker |
| Stream Processing   | Apache Spark 3.5 Structured Streaming, PySpark   |
| Storage & Lakehouse | AWS S3 (Bronze & Silver Parquet), AWS Glue, AWS Redshift |
| Querying            | AWS Athena (Serverless SQL)                      |
| Visualization       | Amazon QuickSight                                |
| Containerization    | Docker, Docker Compose (`datamasterylab` bridge) |
| Governance & Auth   | AWS IAM Roles, Environment Credentials           |

---

## 🧪 Use Case Scenario

The simulated use case tracks a connected vehicle (`Vehicle-123`) traveling the 100-segment corridor from London to Birmingham while emitting:

- Vehicle speed (`mph`) & OBD-II telemetry
- Synchronized GPS coordinates (`latitude`, `longitude`, `direction`)
- Roadside camera snapshots (`Nikon-DC8725` S3 media URI pointers)
- Localized weather reports (`temperature`, `precipitation`, `windSpeed`, `humidity`, `airQualityIndex`)
- Emergency incidents (`Accident`, `Fire`, `Medical`, `Police`, `None` + `Active`/`Resolved` status)

---

## 🔧 System Architecture

<p align="center">
  <img src="Architecture.png" width="650">
  <br />
  <em>AWS, Spark, Kafka & ZooKeeper solution architecture</em>
</p>

- **Kafka & ZooKeeper (Dockerized)**: Stream vehicle, GPS, camera, weather, and emergency data from Python IoT producers.
- **Apache Spark 3.5**: Real-time stream processing cluster (1 Master + 2 Worker nodes) writing Bronze pass-through streams and a joined Silver corridor stream.
- **AWS Cloud Services**:
  - **S3** for raw Bronze, joined Silver Parquet storage, and write-ahead stream checkpoints.
  - **Glue & Crawlers** for automated schema discovery and cataloging.
  - **Athena** for serverless ad-hoc SQL querying over S3 Parquet.
  - **Redshift** for dimensional star-schema warehousing.
  - **QuickSight** for real-time city operations dashboards.

---

## 🚀 Getting Started

### 1. Clone the Repository
```bash
git clone https://github.com/siza94/Smart-City-AWS-Streaming-Data-Engineering-Project.git
cd Smart-City-AWS-Streaming-Data-Engineering-Project
```

### 2. Start Kafka, ZooKeeper, and Spark Cluster
```bash
docker-compose up -d
```

### 3. Start the IoT Telemetry Producer & Submit the Spark Streaming Job
Run the Python producer to stream London–Birmingham telemetry into Kafka:
```bash
python jobs/main.py
```

Submit the Spark Structured Streaming job across the cluster:
```bash
docker exec -it smart_city-spark-master-1 spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469 \
  jobs/spark-city.py
```

---

## 📊 Dashboard (Amazon QuickSight)

<p align="center">
  <img src="City_Analysis.png" width="650">
  <br />
  <em>Amazon QuickSight Operational Dashboard</em>
</p>

---

## ❄️ Entity Relationship Diagram (ERD)

<p align="center">
  <img src="ERD_Diagram.png" width="600">
  <br />
  <em>Star Schema ERD Diagram</em>
</p>

---

## 📘 Interactive Documentation & Portfolio Artifacts

- 🎯 **[Executive & Recruiter Showcase Deck (`smart_city_showcase_deck.html`)](https://siza94.github.io/Smart-City-AWS-Streaming-Data-Engineering-Project/)**
- 📘 **[8-Phase Engineering Lifecycle Report (`smart_city_project_report.html`)](https://siza94.github.io/Smart-City-AWS-Streaming-Data-Engineering-Project/smart_city_project_report.html)**

---

## 👨🏽‍💻 Author

**Sithsaba Zantsi**  
_Data Engineer_  
📫 [Connect on LinkedIn](https://www.linkedin.com/in/sithsaba-zantsi/)

---

## 📜 License

This project is for educational and portfolio demonstration purposes and does not hold any proprietary data or licensing constraints.
