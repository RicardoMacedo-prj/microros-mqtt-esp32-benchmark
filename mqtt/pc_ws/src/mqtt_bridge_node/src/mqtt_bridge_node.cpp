#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <rclcpp/rclcpp.hpp>
#include <std_msgs/msg/string.hpp>
#include <std_msgs/msg/empty.hpp>
#include <custom_msgs/msg/timestamp.hpp>
#include <custom_msgs/msg/latency_report.hpp>

#include <mosquitto.h>

#include "mqtt_bridge_node/protocol.hpp"


/*
 * Bidirectional bridge between ROS 2 topics and MQTT network.
 * Translates control messages, timestamps, and latency reports between ROS 2 and MQTT.
 */
class MqttBridge : public rclcpp::Node
{
private:
  struct mosquitto *mosq_;
  std::mutex map_mutex_;

  // Publishers for messages received from MQTT nodes
  rclcpp::Publisher<std_msgs::msg::String>::SharedPtr pub_discovery_;
  rclcpp::Publisher<custom_msgs::msg::Timestamp>::SharedPtr pub_timestamp_be_;
  rclcpp::Publisher<custom_msgs::msg::Timestamp>::SharedPtr pub_timestamp_rel_;
  rclcpp::Publisher<custom_msgs::msg::LatencyReport>::SharedPtr pub_feedback_;

  // Subscriptions for benchmark control messages from RelayNode
  rclcpp::Subscription<std_msgs::msg::Empty>::SharedPtr sub_wakeup_;
  rclcpp::Subscription<std_msgs::msg::Empty>::SharedPtr sub_finished_;
  rclcpp::Subscription<std_msgs::msg::Empty>::SharedPtr sub_request_feedback_;

  // Per-device subscriptions created dynamically on discovery
  std::unordered_map<std::string, rclcpp::Subscription<std_msgs::msg::String>::SharedPtr> sub_activate_;
  std::unordered_map<std::string, rclcpp::Subscription<custom_msgs::msg::Timestamp>::SharedPtr> sub_timestamp_echo_be_;
  std::unordered_map<std::string, rclcpp::Subscription<custom_msgs::msg::Timestamp>::SharedPtr> sub_timestamp_echo_rel_;

  /*
   * Mosquitto connection callback. Subscribes to all ESP32 topics (esp32/#) upon connection.
   */
  static void on_connect_cb(struct mosquitto *mosq, void *obj, int /*reason_code*/)
  {
    auto *node = static_cast<MqttBridge *>(obj);
    RCLCPP_INFO(node->get_logger(), "Connected to the broker. Subscribing to esp32/#");
    mosquitto_subscribe(mosq, NULL, "esp32/#", 1);
  }

  /*
   * Mosquitto message callback. Translates incoming MQTT binary payloads to ROS 2 messages.
   */
  static void on_message_cb(struct mosquitto * /*mosq*/, void *obj, const struct mosquitto_message *msg)
  {
    auto *node = static_cast<MqttBridge *>(obj);
    std::string topic(msg->topic);
    const char *payload = static_cast<const char *>(msg->payload);

    // Register discovered device and set up communication routes
    if (topic == "esp32/discovery")
    {
      std_msgs::msg::String ros_msg;
      std::size_t safe_len = std::min(static_cast<std::size_t>(msg->payloadlen), mqtt_bridge::DEVICE_ID_SIZE);
      ros_msg.data = std::string(payload, safe_len);
      node->pub_discovery_->publish(ros_msg);
      node->register_device(ros_msg.data);
    }
    // Unpack binary payload and publish timestamp to ROS 2
    else if (topic == "esp32/timestamp_be" || topic == "esp32/timestamp_rel")
    {
      if (msg->payloadlen != mqtt_bridge::TIMESTAMP_PAYLOAD_SIZE) {
        return;
      }

      custom_msgs::msg::Timestamp ros_msg;
      char dev_id[mqtt_bridge::DEVICE_ID_SIZE + 1] = {0};
      std::memcpy(dev_id, payload, mqtt_bridge::DEVICE_ID_SIZE);
      ros_msg.device_id = std::string(dev_id);

      std::memcpy(&ros_msg.timestamp, payload + mqtt_bridge::DEVICE_ID_SIZE, sizeof(int64_t));

      if (topic.find("_be") != std::string::npos) {
        node->pub_timestamp_be_->publish(ros_msg);
      } else {
        node->pub_timestamp_rel_->publish(ros_msg);
      }
    }
    // Validate packet size, extract telemetry, and publish latency report
    else if (topic == "esp32/feedback")
    {
      if (static_cast<std::size_t>(msg->payloadlen) < mqtt_bridge::LATENCY_HEADER_SIZE) {
        return;
      }

      uint32_t sample_count = 0;
      std::memcpy(&sample_count, payload + mqtt_bridge::DEVICE_ID_SIZE + sizeof(uint32_t), sizeof(uint32_t));

      // Verify payload length against reported sample count to reject malformed packets
      std::size_t expected_payload_size = mqtt_bridge::LATENCY_HEADER_SIZE + (sample_count * sizeof(int32_t));
      if (static_cast<std::size_t>(msg->payloadlen) < expected_payload_size) {
        RCLCPP_WARN(node->get_logger(), "Feedback packet is truncated or corrupted. Discarded.");
        return;
      }

      custom_msgs::msg::LatencyReport ros_msg;
      char dev_id[mqtt_bridge::DEVICE_ID_SIZE + 1] = {0};
      std::memcpy(dev_id, payload, mqtt_bridge::DEVICE_ID_SIZE);
      ros_msg.device_id = std::string(dev_id);

      std::memcpy(&ros_msg.sample_block, payload + mqtt_bridge::DEVICE_ID_SIZE, sizeof(uint32_t));

      if (sample_count > 0 && sample_count <= mqtt_bridge::MAX_LATENCY_SAMPLES) {
        ros_msg.latency_us.resize(sample_count);
        std::memcpy(&ros_msg.latency_us[0], payload + mqtt_bridge::LATENCY_HEADER_SIZE, sample_count * sizeof(int32_t));
      }

      node->pub_feedback_->publish(ros_msg);
    }
  }

  /*
   * Registers dynamic ROS 2 subscriptions to transmit configuration and timestamp echoes to MQTT.
   */
  void register_device(const std::string &device_id)
  {
    std::lock_guard<std::mutex> lock(map_mutex_);

    if (sub_activate_.find(device_id) == sub_activate_.end())
    {
      RCLCPP_INFO(this->get_logger(), "Registering routes for: %s", device_id.c_str());

      // Transmit device configuration to MQTT (QoS 1)
      sub_activate_[device_id] = this->create_subscription<std_msgs::msg::String>(
        "/" + device_id + "/activate", 10,
        [this, device_id](const std_msgs::msg::String::SharedPtr msg)
        {
          std::string topic = "pc/" + device_id + "/activate";
          mosquitto_publish(mosq_, NULL, topic.c_str(), msg->data.length(), msg->data.c_str(), 1, false);
        });

      // Serializes Timestamp message to binary payload and publishes over MQTT
      auto timestamp_callback = [this, device_id](const custom_msgs::msg::Timestamp::SharedPtr msg, int qos, const std::string &suffix)
      {
        char tx_buf[mqtt_bridge::TIMESTAMP_PAYLOAD_SIZE] = {0};
        std::memcpy(tx_buf, msg->device_id.c_str(), std::min(msg->device_id.length(), mqtt_bridge::DEVICE_ID_SIZE));
        std::memcpy(tx_buf + mqtt_bridge::DEVICE_ID_SIZE, &msg->timestamp, sizeof(int64_t));

        std::string topic = "pc/" + device_id + "/timestamp_" + suffix;
        mosquitto_publish(mosq_, NULL, topic.c_str(), mqtt_bridge::TIMESTAMP_PAYLOAD_SIZE, tx_buf, qos, false);
      };

      // Transmit returned timestamps back to ESP32 via MQTT using QoS 0 (At most once)
      sub_timestamp_echo_be_[device_id] = this->create_subscription<custom_msgs::msg::Timestamp>(
        "/" + device_id + "/timestamp_be", 
        rclcpp::QoS(2000).best_effort(),
        [timestamp_callback](const custom_msgs::msg::Timestamp::SharedPtr msg)
        {
          timestamp_callback(msg, 0, "be");
        });

      // Transmit returned timestamps back to ESP32 via MQTT using QoS 1 (At least once)
      sub_timestamp_echo_rel_[device_id] = this->create_subscription<custom_msgs::msg::Timestamp>(
        "/" + device_id + "/timestamp_rel", 
        rclcpp::QoS(2000).reliable(),
        [timestamp_callback](const custom_msgs::msg::Timestamp::SharedPtr msg)
        {
          timestamp_callback(msg, 1, "rel");
        });
    }
  }

public:
  /*
   * Initializes ROS 2 endpoints, establishes MQTT client connection, and starts the network thread.
   */
  MqttBridge() : Node("mqtt_bridge")
  {
    // Publishers for messages received from MQTT
    pub_discovery_ = this->create_publisher<std_msgs::msg::String>("/discovery", rclcpp::QoS(100).reliable());
    pub_timestamp_be_ = this->create_publisher<custom_msgs::msg::Timestamp>("/timestamp_be", rclcpp::QoS(2000).best_effort());
    pub_timestamp_rel_ = this->create_publisher<custom_msgs::msg::Timestamp>("/timestamp_rel", rclcpp::QoS(2000).reliable());
    pub_feedback_ = this->create_publisher<custom_msgs::msg::LatencyReport>("/feedback", rclcpp::QoS(500).reliable());

    // Subscription to publish wake-up signal to MQTT
    sub_wakeup_ = this->create_subscription<std_msgs::msg::Empty>(
      "/wakeup", 10,
      [this](const std_msgs::msg::Empty::SharedPtr) {
        mosquitto_publish(mosq_, NULL, "pc/wakeup", 0, "", 1, false);
      });

    // Subscription to publish test completion signal to MQTT
    sub_finished_ = this->create_subscription<std_msgs::msg::Empty>(
      "/test_finished", 10,
      [this](const std_msgs::msg::Empty::SharedPtr) {
        mosquitto_publish(mosq_, NULL, "pc/test_finished", 0, "", 1, false);
      });

    // Subscription to publish feedback request to MQTT
    sub_request_feedback_ = this->create_subscription<std_msgs::msg::Empty>(
      "/request_feedback", 10,
      [this](const std_msgs::msg::Empty::SharedPtr) {
        mosquitto_publish(mosq_, NULL, "pc/request_feedback", 0, "", 1, false);
      });

    // Initialize Mosquitto library and allocate client instance
    mosquitto_lib_init();
    mosq_ = mosquitto_new("ROS2_Bridge", true, this);

    // Configure TCP socket to disable buffering delay
    int nodelay = 1;
    mosquitto_opts_set(mosq_, MOSQ_OPT_TCP_NODELAY, &nodelay);

    // Register Mosquitto event callbacks
    mosquitto_connect_callback_set(mosq_, on_connect_cb);
    mosquitto_message_callback_set(mosq_, on_message_cb);
    
    // Connect to local broker and start background network processing loop
    if (mosquitto_connect(mosq_, "localhost", 1883, 60) == MOSQ_ERR_SUCCESS) {
      mosquitto_loop_start(mosq_);
    } else {
      RCLCPP_ERROR(this->get_logger(), "TCP Broker connection failed");
    }
  }

  /*
   * Stops MQTT network loop, disconnects from broker, and frees client resources.
   */
  ~MqttBridge()
  {
    mosquitto_loop_stop(mosq_, true);
    mosquitto_disconnect(mosq_);
    mosquitto_destroy(mosq_);
    mosquitto_lib_cleanup();
  }
};

/*
 * Initializes ROS 2 runtime and executes the MQTT bridge node.
 */
int main(int argc, char *argv[])
{
  rclcpp::init(argc, argv);
  rclcpp::spin(std::make_shared<MqttBridge>());
  rclcpp::shutdown();
  return 0;
}