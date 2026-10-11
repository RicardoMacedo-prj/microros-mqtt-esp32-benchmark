# micro-ROS vs MQTT ROS Bridge: ESP32 Benchmark

Hardware-in-the-Loop benchmark suite designed to evaluate and compare the performance, latency, CPU utilization, and throughput between **micro-ROS** and an **MQTT-to-ROS 2 Bridge** using ESP32 microcontrollers connected to a Linux host running ROS 2 Humble.


## Communication Architecture

Both architectures establish bidirectional communication between ESP32 microcontrollers and the ROS 2 host:

* **micro-ROS:** UDP communication (port 8888) between ESP32 clients and the `micro_ros_agent`. Messages are forwarded to and echoed back by the C++ Relay Node (`relay_node`).
* **MQTT Bridge:** TCP communication (port 1883) between ESP32 clients and an Eclipse Mosquitto broker. The `mqtt_bridge_node` translates messages between MQTT and ROS 2 topics, routing traffic to and from the C++ Relay Node (`relay_node`).


## Test Matrix and Metrics

The automated Python scripts execute tests across a predefined matrix:
- **Batch Size:** Configurable message count (default: 10,000 messages per test).
- **Frequencies:** 50 Hz, 100 Hz, 200 Hz.
- **Device Scaling:** 1, 3, and 5 concurrent ESP32 nodes.
- **QoS Modes:**
  - Mode 0: Best Effort (micro-ROS) / QoS 0 (MQTT).
  - Mode 1: Reliable (micro-ROS) / QoS 1 (MQTT).
- **Recorded Metrics:**
  - Round-trip latency (microseconds).
  - Effective throughput (messages per second).
  - Execution time (milliseconds).
  - ESP32 CPU load (percentage).
  - Failure count per run.

Results are held in memory during execution and saved to CSV files in `results/` upon confirmed test completion.


## Repository Structure

```text
.
├── mqtt/
│   ├── esp32_node/                # ESP-IDF firmware for MQTT nodes
│   │   ├── main/
│   │   ├── CMakeLists.txt
│   │   └── sdkconfig.defaults
│   └── pc_ws/                     # ROS 2 workspace for MQTT
│       ├── config/
│       │   └── mosquitto.conf     # Mosquitto broker configuration
│       ├── results/
│       │   └── mqtt_benchmarks_log.csv  # Benchmark results log
│       ├── scripts/
│       │   └── run_mqtt_benchmarks.py   # Benchmark execution script
│       └── src/
│           ├── custom_msgs/       # LatencyReport and Timestamp messages
│           ├── mqtt_bridge_node/  # Custom ROS 2 to MQTT Bridge node
│           └── pc_node/           # ROS 2 relay node
├── uros/
│   ├── esp32_node/                # ESP-IDF firmware for micro-ROS nodes
│   │   ├── main/
│   │   ├── CMakeLists.txt
│   │   └── sdkconfig.defaults
│   └── pc_ws/                     # ROS 2 workspace for micro-ROS
│       ├── results/
│       │   └── uros_benchmarks_log.csv  # Benchmark results log
│       ├── scripts/
│       │   └── run_uros_benchmarks.py   # Benchmark execution script
│       └── src/
│           ├── pc_node/           # ROS 2 relay node
│           └── uros_custom_msgs/  # LatencyReport and Timestamp messages
├── LICENSE
└── README.md
```

## Prerequisites

* **Operating System:** Ubuntu 22.04 LTS
* **ROS Distribution:** ROS 2 Humble
* **Embedded Framework:** ESP-IDF
* **MQTT Broker:** Eclipse Mosquitto (`sudo apt install mosquitto`)
* **micro-ROS Agent:** `micro_ros_agent` ROS 2 package
* **Python Runtime:** Python 3.10+


## Build Instructions

### Build MQTT Host Workspace
```bash
cd mqtt/pc_ws
source /opt/ros/humble/setup.bash
colcon build
```

### Build micro-ROS Host Workspace
```bash
cd uros/pc_ws
source /opt/ros/humble/setup.bash
colcon build
```

### Flash ESP32 Nodes
Each node requires network credentials and the Host PC IP address. Device identification is automatically derived from the factory MAC address.

* Wi-Fi SSID
* Wi-Fi Password
* Host PC IP Address (running the broker or micro-ROS agent)

#### MQTT Nodes
Configure network parameters directly in the firmware source file before building:
```bash
cd mqtt/esp32_node
# Edit WIFI_SSID, WIFI_PASS, and MQTT_BROKER_URI in main/main.c
idf.py build flash -p <PORT>
```

#### micro-ROS Nodes
Configure network parameters via Kconfig menu:
```bash
cd uros/esp32_node
idf.py menuconfig
# Set Wi-Fi credentials and Agent IP under the project configuration menu
idf.py build flash -p <PORT>
```


## Execution

Ensure that the ESP32 nodes and the host machine are connected to the same Wi-Fi network.

### Run MQTT Benchmark
Navigate to the workspace and source the built environment:
```bash
cd mqtt/pc_ws
source install/setup.bash
```

Execute the complete default test matrix:
```bash
python3 scripts/run_mqtt_benchmarks.py
```

Or execute with parameter filters:
```bash
# Filter by specific frequency, QoS, or node count
python3 scripts/run_mqtt_benchmarks.py --hz 100 --nodes 3 --qos 1
```

### Run micro-ROS Benchmark
Navigate to the workspace and source the built environment:
```bash
cd uros/pc_ws
source install/setup.bash
```

Execute the complete default test matrix:
```bash
python3 scripts/run_uros_benchmarks.py
```

Or execute with parameter filters:
```bash
# Filter by specific frequency, QoS, or node count
python3 scripts/run_uros_benchmarks.py --hz 100 --nodes 3 --qos 1
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.