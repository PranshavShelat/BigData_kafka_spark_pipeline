# Distributed Server Monitoring Pipeline using Kafka and PySpark

## Overview
A distributed data processing pipeline for near real-time server health monitoring and anomaly detection. The system ingests simulated telemetry logs via Apache Kafka and processes them using Apache Spark (PySpark) to detect sustained hardware stress and potential cybersecurity threats (e.g., DDoS attacks, disk thrashing).

## Architecture
The system operates across four virtual machines connected via a ZeroTier network:

* **Node 1: Kafka Broker (192.168.191.142)**
  Runs Apache ZooKeeper and the Kafka Server. Maintains four dedicated topics: `topic-cpu`, `topic-mem`, `topic-net`, and `topic-disk`.
* **Node 2: Data Producer**
  Reads raw server logs from a dataset, serializes the records into JSON, and publishes them to the Kafka broker.
* **Node 3: Consumer 1 (CPU & Memory)**
  Subscribes to CPU and Memory topics, writing the stream to local CSVs. Runs a PySpark job to analyze the data for hardware stress.
* **Node 4: Consumer 2 (Network & Disk)**
  Subscribes to Network and Disk topics, writing the stream to local CSVs. Runs a PySpark job to detect abnormal network traffic and disk I/O.

## Methodology
To prevent false positives from momentary metric spikes, the PySpark jobs utilize a time-series analysis with a **30-second sliding window** that updates every 10 seconds. 

Data is aggregated before threshold evaluation:
* **CPU and Memory:** Evaluated using window averages.
* **Network and Disk:** Evaluated using window maximums.
Alerts are formatted with `HH:MM:SS` timestamps and generated only if aggregated metrics exceed the predefined thresholds in `thresholds.txt`.
```
## Repository Structure
├── README.md
├── team_23_broker.txt                # Kafka setup and topic configurations
├── thresholds.txt                    # Alerting threshold values
├── src/
│   ├── team_23_producer.py           # Ingests CSV and streams JSON to Kafka
│   ├── team_23_consumer1.py          # Consumes CPU/MEM topics
│   ├── team_23_consumer2.py          # Consumes NET/DISK topics
│   ├── team_23_sparkjob1.py          # PySpark CPU/MEM analysis
│   └── team_23_sparkjob2.py          # PySpark NET/DISK analysis
├── data/
│   └── dataset_sample.csv            # 100-row sample of raw telemetry data
├── output/
│   ├── team_23_CPU_MEM.csv           # Final CPU/Memory anomaly alerts
│   └── team_23_NET_DISK.csv          # Final Network/Disk anomaly alerts
└── images/
    ├── team_23_MEMVerification.png   # Execution proof
    ├── team_23_NETVerification.png   # Execution proof
    └── team_23_ZeroTier.jpg          # Network configuration proof
```
## Execution Steps

**1. Initialize Broker (Node 1)**
Start ZooKeeper and Kafka, then create the four topics:
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
bin/kafka-topics.sh --create --topic topic-cpu --bootstrap-server 192.168.191.142:9092
# Repeat for topic-mem, topic-net, and topic-disk

**2. Start Consumers (Nodes 3 & 4)**
Initialize the consumer scripts to listen for incoming data:
python3 src/team_23_consumer1.py
python3 src/team_23_consumer2.py

**3. Run Producer (Node 2)**
Stream the dataset into the Kafka pipeline:
python3 src/team_23_producer.py
(Wait for completion, then stop the consumer scripts with Ctrl+C).

**4. Execute PySpark Analysis (Nodes 3 & 4)**
Run the Spark jobs to generate the final alert CSVs:
spark-submit src/team_23_sparkjob1.py
spark-submit src/team_23_sparkjob2.py
