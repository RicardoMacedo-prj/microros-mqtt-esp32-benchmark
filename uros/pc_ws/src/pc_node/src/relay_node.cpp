#include <algorithm>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <rclcpp/rclcpp.hpp>
#include <std_srvs/srv/trigger.hpp>
#include <std_msgs/msg/string.hpp>
#include <std_msgs/msg/empty.hpp>
#include <uros_custom_msgs/msg/timestamp.hpp>
#include <uros_custom_msgs/msg/latency_report.hpp>

/*
 * Coordinates multi-node benchmark execution. Manages device discovery,
 * configuration dispatch, timestamp echo routing, and telemetry aggregation.
 */
class RelayNode : public rclcpp::Node
{
  int64_t number_of_devices_to_activate_;
  int64_t active_devices_count_ = 0;
  int64_t expected_total_samples_;

  int64_t current_hz_;
  int64_t qos_mode_;

  std::vector<std::string> discovered_devices_;
  std::vector<std::string> feedback_received_;
  std::vector<std::string> execution_finished_devices_;

  rclcpp::TimerBase::SharedPtr discovery_timer_;
  rclcpp::TimerBase::SharedPtr test_timeout_timer_;

  std::unordered_map<std::string, size_t> device_samples_count_;
  std::unordered_map<std::string, float> cpu_map_;
  std::unordered_map<std::string, float> throughput_map_;
  std::unordered_map<std::string, int32_t> exec_time_map_;

  // Subscribers for execution, discovery and feedback messages
  rclcpp::Subscription<uros_custom_msgs::msg::Timestamp>::SharedPtr execution_sub_be_;
  rclcpp::Subscription<uros_custom_msgs::msg::Timestamp>::SharedPtr execution_sub_rel_;
  rclcpp::Subscription<std_msgs::msg::String>::SharedPtr discovery_sub_;
  rclcpp::Subscription<uros_custom_msgs::msg::LatencyReport>::SharedPtr feedback_sub_;

  // Publishers for wakeup, experiment completion and feedback request messages
  rclcpp::Publisher<std_msgs::msg::Empty>::SharedPtr wakeup_pub_;
  rclcpp::Publisher<std_msgs::msg::Empty>::SharedPtr test_finished_pub_;
  rclcpp::Publisher<std_msgs::msg::Empty>::SharedPtr request_feedback_pub_;

  // Map to hold publishers for different device IDs, allowing dynamic topic creation
  std::unordered_map<
      std::string,
      rclcpp::Publisher<uros_custom_msgs::msg::Timestamp>::SharedPtr
  > execution_pubs_be_;

  std::unordered_map<
      std::string,
      rclcpp::Publisher<uros_custom_msgs::msg::Timestamp>::SharedPtr
  > execution_pubs_rel_;

  std::unordered_map<
      std::string,
      rclcpp::Publisher<std_msgs::msg::String>::SharedPtr
  > activation_pubs_;

  rclcpp::Service<std_srvs::srv::Trigger>::SharedPtr start_service_;

  // Relay node state machine
  enum class State
  {
    IDLE,
    DISCOVERING,
    ACTIVATING,
    RUNNING,
    RESETTING
  };

  State current_state_ = State::IDLE;
  
  public:
    RelayNode()
    : Node("relay_node")
    {
      // Benchmark configuration parameters
      this->declare_parameter("number_of_devices_to_activate", 1);
      this->declare_parameter("current_hz", 50);
      this->declare_parameter("expected_total_samples", 10000);
      this->declare_parameter("qos_mode", 0);

      // Subscriptions to receive timestamps from ESP32 nodes
      execution_sub_be_ = this->create_subscription<uros_custom_msgs::msg::Timestamp>(
        "/timestamp_be_tx", 
        rclcpp::QoS(2000).best_effort(),
        std::bind(&RelayNode::execution_callback, this, std::placeholders::_1));

      execution_sub_rel_ = this->create_subscription<uros_custom_msgs::msg::Timestamp>(
        "/timestamp_rel_tx", 
        rclcpp::QoS(2000).reliable(),
        std::bind(&RelayNode::execution_callback, this, std::placeholders::_1));
      
      // Node discovery subscription
      discovery_sub_ = this->create_subscription<std_msgs::msg::String>(
        "/discovery",
        rclcpp::QoS(10).reliable(),
        std::bind(&RelayNode::discovery_callback, this, std::placeholders::_1));
      
      // Telemetry and latency feedback subscription
      feedback_sub_ = this->create_subscription<uros_custom_msgs::msg::LatencyReport>(
        "/feedback",
        rclcpp::QoS(500).reliable(),
        std::bind(&RelayNode::feedback_callback, this, std::placeholders::_1));
      
      // Benchmark control publishers
      wakeup_pub_ = this->create_publisher<std_msgs::msg::Empty>("/wakeup", 10);
      test_finished_pub_ = this->create_publisher<std_msgs::msg::Empty>("/test_finished", 10);
      request_feedback_pub_ = this->create_publisher<std_msgs::msg::Empty>("/request_feedback", 10);

      // Service to trigger benchmark start
      start_service_ = this->create_service<std_srvs::srv::Trigger>(
        "/start_test",
        std::bind(
            &RelayNode::start_callback,
            this,
            std::placeholders::_1,
            std::placeholders::_2));
    }

    /*
     * Sends wake-up command and starts node discovery.
     */
    void trigger_wakeup()
    {
      if (current_state_ != State::IDLE) {
        return;
      }

      // Broadcast wake-up message so nodes publish their IDs
      std_msgs::msg::Empty wakeup_msg;
      RCLCPP_INFO(this->get_logger(), "Sending wakeup messages");
      wakeup_pub_->publish(wakeup_msg);

      current_state_ = State::DISCOVERING;

      // 5-second timer to discover all active nodes
      discovery_timer_ = this->create_wall_timer(
        std::chrono::seconds(5),
        std::bind(&RelayNode::finish_discovery, this));

      check_state();
    }

    /*
     * Completes discovery phase and transitions to device activation.
     */
    void finish_discovery()
    {
      if (current_state_ != State::DISCOVERING) {
        return;
      }

      if (discovery_timer_) {
        discovery_timer_->cancel();
      }

      // Sort device IDs to ensure deterministic activation order
      std::sort(discovered_devices_.begin(), discovered_devices_.end());

      RCLCPP_INFO(
        this->get_logger(),
        "Discovery finished. Devices found: %zu",
        discovered_devices_.size());

      for (const auto &device : discovered_devices_) {
        RCLCPP_INFO(this->get_logger(), "Device: %s", device.c_str());
      }

      current_state_ = State::ACTIVATING;
      check_state();
    }

    /*
     * Validates discovered devices, dispatches configuration parameters,
     * and starts the test timeout timer.
     */
    void activate_devices()
    {
      const size_t TOTAL_PHYSICAL_DEVICES = 5;

      if (current_state_ != State::ACTIVATING) {
        return;
      }
      
      // Abort experiment if any physical device failed to report during discovery
      if (discovered_devices_.size() != TOTAL_PHYSICAL_DEVICES) {
        RCLCPP_WARN(
          this->get_logger(),
          "Insufficient devices discovered: expected %zu, but found %zu.",
          TOTAL_PHYSICAL_DEVICES,
          discovered_devices_.size());

        current_state_ = State::RESETTING;
        check_state();
        return;
      }

      size_t amount = std::min(
        static_cast<size_t>(number_of_devices_to_activate_),
        discovered_devices_.size()
      );

      // Send configuration to each selected device
      for (size_t i = 0; i < amount; i++) {
        const auto &device = discovered_devices_[i];
        auto it = activation_pubs_.find(device);

        if (it != activation_pubs_.end()) {
          std_msgs::msg::String activation_msg;
          
          // Payload format: frequency, total samples, QoS mode
          activation_msg.data = std::to_string(current_hz_) + "," +
                                std::to_string(expected_total_samples_) + "," +
                                std::to_string(qos_mode_);

          RCLCPP_INFO(this->get_logger(), "Sending configuration to %s: %s", device.c_str(), activation_msg.data.c_str());
          it->second->publish(activation_msg);

          // Stagger activation commands to avoid Wi-Fi channel contention
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
      } 

      active_devices_count_ = amount;

      // Calculate test timeout from expected duration and safety margin
      int base_time_seconds = expected_total_samples_ / current_hz_;
      int timeout_seconds = (base_time_seconds * 4) + 30;

      // Timer to abort test if timeout expires
      test_timeout_timer_ = this->create_wall_timer(
        std::chrono::seconds(timeout_seconds),
        std::bind(&RelayNode::force_finish_test, this));

      current_state_ = State::RUNNING;
    }

    /*
     * Resets timers, clears telemetry buffers, and returns to IDLE state.
     */
    void reset_experiment()
    {
      if (discovery_timer_) {
        discovery_timer_->cancel();
        discovery_timer_.reset();
      }

      if (test_timeout_timer_) {
        test_timeout_timer_->cancel();
        test_timeout_timer_.reset();
      }

      // Clear discovered devices and telemetry data
      discovered_devices_.clear();
      feedback_received_.clear();
      execution_finished_devices_.clear();

      active_devices_count_ = 0;
      device_samples_count_.clear();

      cpu_map_.clear();
      throughput_map_.clear();
      exec_time_map_.clear();

      current_state_ = State::IDLE;

      RCLCPP_INFO(this->get_logger(), "Relay node is ready for a new experiment.");
    }

    /*
     * Aborts test on timeout and broadcasts finish signal to all nodes.
     */
    void force_finish_test()
    {
      if (current_state_ != State::RUNNING) {
        return;
      }

      if (test_timeout_timer_) {
        test_timeout_timer_->cancel();
      }

      RCLCPP_WARN(this->get_logger(), "Test timeout. End test signal lost. Forcing finish.");

      // Send repeated finish messages to ensure delivery over Wi-Fi
      std_msgs::msg::Empty finished_msg;
      for (int i = 0; i < 10; i++) {
        test_finished_pub_->publish(finished_msg);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
      }

      // Allow network buffers to clear before resetting state
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      current_state_ = State::RESETTING;
      check_state();
    }

    /*
     * Executes actions associated with state transitions.
     */
    void check_state()
    {
      switch (current_state_)
      {
        case State::IDLE:
          // Awaiting /start_test service request
          break;

        case State::DISCOVERING:
          // Nodes register via discovery_callback until discovery_timer_ expires
          break;

        case State::ACTIVATING:
          // Dispatch test configuration to selected devices
          activate_devices();
          break;

        case State::RUNNING:
          // Test in progress; processing timestamp echoes and feedback reports
          break;

        case State::RESETTING:
          // Clear experiment data and restore initial node state
          reset_experiment();
          break;
      }
    }
  
  private:
    /*
     * Handles /start_test service request to configure and start a new benchmark run.
     */
    void start_callback(
      const std::shared_ptr<std_srvs::srv::Trigger::Request>,
      std::shared_ptr<std_srvs::srv::Trigger::Response> response)
    {
      // Reject request if a test is already in progress
      if (current_state_ != State::IDLE) {
        response->success = false;
        response->message = "Relay is not idle";
        return;
      }

      // Read benchmark parameters for this run
      this->get_parameter("number_of_devices_to_activate", number_of_devices_to_activate_);
      this->get_parameter("current_hz", current_hz_);
      this->get_parameter("expected_total_samples", expected_total_samples_);
      this->get_parameter("qos_mode", qos_mode_);

      trigger_wakeup();

      response->success = true;
      response->message = "Test started";
    }
    
    /*
     * Registers discovered device IDs and initializes per-device publishers.
     */
    void discovery_callback(const std_msgs::msg::String::SharedPtr msg)
    {
      if (msg->data.empty() || current_state_ != State::DISCOVERING) {
        return;
      }

      // Register device ID if not already discovered
      if (std::find(discovered_devices_.begin(), discovered_devices_.end(), msg->data) == discovered_devices_.end()) {
        discovered_devices_.push_back(msg->data);
      }

      // Initialize per-device publishers on first discovery
      auto it = activation_pubs_.find(msg->data);
      if (it == activation_pubs_.end()) {
        std::string topic = "/" + msg->data + "/activate";
        auto pub = this->create_publisher<std_msgs::msg::String>(topic, 10);
        activation_pubs_.emplace(msg->data, pub);
        
        // Unicast echo publisher (Best Effort)
        std::string topic_be = "/" + msg->data + "/timestamp_be_rx";
        auto exec_pub_be = this->create_publisher<uros_custom_msgs::msg::Timestamp>(
          topic_be, rclcpp::QoS(100).best_effort());
        execution_pubs_be_.emplace(msg->data, exec_pub_be);

        // Unicast echo publisher (Reliable)
        std::string topic_rel = "/" + msg->data + "/timestamp_rel_rx";
        auto exec_pub_rel = this->create_publisher<uros_custom_msgs::msg::Timestamp>(
          topic_rel, rclcpp::QoS(100).reliable());
        execution_pubs_rel_.emplace(msg->data, exec_pub_rel);
      }
    }

    /*
     * Echoes received timestamps back to the originating device
     * using the configured QoS profile.
     */
    void execution_callback(const uros_custom_msgs::msg::Timestamp::SharedPtr msg)
    {
      if (msg->device_id.empty() || current_state_ != State::RUNNING) {
        return;
      }

      // Echo timestamp using the selected profile (0: Best Effort, 1: Reliable)
      if (qos_mode_ == 0) 
      {
        auto it = execution_pubs_be_.find(msg->device_id);
        if (it != execution_pubs_be_.end()) {
          it->second->publish(*msg);
        } 
      } 
      else 
      {
        auto it = execution_pubs_rel_.find(msg->device_id);
        if (it != execution_pubs_rel_.end()) {
          it->second->publish(*msg);
        }
      }
    }

    /*
     * Processes latency reports, device telemetry, and test completion signals.
     */
    void feedback_callback(const uros_custom_msgs::msg::LatencyReport::SharedPtr msg)
    {
      if (msg->device_id.empty() || current_state_ != State::RUNNING) {
        return;
      }

      // Reset timeout timer on receiving valid report from an active device
      if (test_timeout_timer_) {
        test_timeout_timer_->reset();
      }

      // Block 998 contains CPU utilization and execution duration
      if (msg->sample_block == 998) {
        if (msg->latency_us.size() < 2) {
          return;
        }

        float received_cpu = (float)msg->latency_us[0] / 100.0f;
        this->cpu_map_[msg->device_id] = received_cpu;

        int32_t duration_ms = msg->latency_us[1];
        this->exec_time_map_[msg->device_id] = duration_ms;

        // Calculate effective frequency from total samples and elapsed execution time
        float execution_hz = 0.0f;
        if (duration_ms > 0) {
          execution_hz = (float)expected_total_samples_ / (duration_ms / 1000.0f);
        }
        this->throughput_map_[msg->device_id] = execution_hz;

        RCLCPP_INFO(
          this->get_logger(), 
          "Device %s | CPU: %.2f%% | Exec Time: %d ms | Throughput: %.2f Hz", 
          msg->device_id.c_str(), received_cpu, duration_ms, execution_hz);

        // Register devices that completed their execution phase
        if (std::find(execution_finished_devices_.begin(), execution_finished_devices_.end(), msg->device_id) == execution_finished_devices_.end()) {
          execution_finished_devices_.push_back(msg->device_id);
        }

        // Request latency reports once all active nodes complete their execution phase
        if (active_devices_count_ > 0 && execution_finished_devices_.size() >= static_cast<size_t>(active_devices_count_)) {
          RCLCPP_INFO(this->get_logger(), "All nodes have completed their main execution. Starting data collection...");
          std_msgs::msg::Empty req_msg;
          request_feedback_pub_->publish(req_msg);
        }

        return;
      }

      // Block 9999 indicates end of report transmission for this device
      if (msg->sample_block == 9999)
      {
        if (std::find(feedback_received_.begin(), feedback_received_.end(), msg->device_id) == feedback_received_.end())
        {
          feedback_received_.push_back(msg->device_id);
          RCLCPP_INFO(
            this->get_logger(),
            "Device %s finished. Missing samples: %zu", 
            msg->device_id.c_str(),
            expected_total_samples_ - device_samples_count_[msg->device_id]);
        }

        // Complete experiment when all active nodes finish data transmission
        if (active_devices_count_ > 0 && feedback_received_.size() >= static_cast<size_t>(active_devices_count_))
        {
          RCLCPP_INFO(this->get_logger(), "Test finished. All devices sent their data.");
          
          // Send repeated finish messages to ensure reception over Wi-Fi
          std_msgs::msg::Empty finished_msg;
          for (int i = 0; i < 10; i++) {
            test_finished_pub_->publish(finished_msg);
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
          }

          // Allow network buffers to clear before resetting node state
          std::this_thread::sleep_for(std::chrono::milliseconds(1000));
          current_state_ = State::RESETTING;
          check_state();
        }
        
        return; 
      }

      if (msg->latency_us.empty()) {
        return;
      }

      // Accumulate received latency samples count
      device_samples_count_[msg->device_id] += msg->latency_us.size();

      // Pre-allocate memory to avoid reallocations during string construction
      std::string latency_str;
      latency_str.reserve(1200);

      for (size_t i = 0; i < msg->latency_us.size(); i++)
      {
        latency_str += std::to_string(msg->latency_us[i]);
        if (i < msg->latency_us.size() - 1) {
          latency_str += ", ";
        }
      }

      float current_cpu = 0.0f;
      if (cpu_map_.find(msg->device_id) != cpu_map_.end()) {
        current_cpu = cpu_map_[msg->device_id];
      }

      float current_throughput = 0.0f;
      if (throughput_map_.find(msg->device_id) != throughput_map_.end()) {
        current_throughput = throughput_map_[msg->device_id];
      }

      int32_t current_exec_time = 0;
      if (exec_time_map_.find(msg->device_id) != exec_time_map_.end()) {
        current_exec_time = exec_time_map_[msg->device_id];
      }

      RCLCPP_INFO(
        this->get_logger(),
        "Device %s | Block %u | CPU: %.2f | Throughput: %.2f | ExecTime: %d | Samples: %zu | Latencies: [%s]",
        msg->device_id.c_str(),
        msg->sample_block,
        current_cpu,
        current_throughput,
        current_exec_time,
        msg->latency_us.size(),
        latency_str.c_str());
    }
};

/*
 * Initializes ROS 2 runtime and spins the relay node.
 */
int main(int argc, char * argv[])
{
  rclcpp::init(argc, argv);
  rclcpp::spin(std::make_shared<RelayNode>());
  rclcpp::shutdown();

  return 0;
}