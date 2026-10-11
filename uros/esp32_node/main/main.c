#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sdkconfig.h>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>

#include <esp_timer.h>
#include <esp_log.h>
#include <esp_mac.h>
#include <esp_random.h>
#include <esp_wifi.h>

#include <uros_network_interfaces.h>
#include <rcl/rcl.h>
#include <rclc/rclc.h>
#include <rclc/executor.h>

#include <rosidl_runtime_c/message_type_support_struct.h>
#include <rosidl_runtime_c/string_functions.h>
#include <rosidl_runtime_c/primitives_sequence_functions.h>

#include <uros_custom_msgs/msg/timestamp.h>
#include <uros_custom_msgs/msg/latency_report.h>
#include <std_msgs/msg/string.h>
#include <std_msgs/msg/empty.h>

#include <rmw_microros/rmw_microros.h>

#define MAX_SAMPLES 12000
#define SAMPLES_PER_BLOCK 100
#define MAX_TASKS_SNAPSHOT 40

// Finite state machine states for the benchmark test cycle
typedef enum
{
    STATE_IDLE,
    STATE_WAITING_ACTIVATION,
    STATE_RUNNING,
    STATE_COOLDOWN,
    STATE_WAITING_REPORT_SYNC,
    STATE_REPORTING,
    STATE_WAITING_FINISH
} app_state_t;

volatile app_state_t current_state = STATE_IDLE;

uint32_t current_hz = 50;
uint32_t target_samples = 0;
uint32_t qos_mode = 0;
uint32_t start_total_time = 0;
uint32_t start_idle_time = 0;
uint8_t my_mac_tail = 0;

char device_namespace[16];
volatile TickType_t activation_start_time = 0;
static int32_t *latency_buffer = NULL;
static size_t current_sample_index = 0;
volatile bool feedback_authorized = false;

// Micro-ROS objects
rclc_support_t support;
rcl_node_t node;
rclc_executor_t executor;
rcl_allocator_t allocator;

// Publishers
rcl_publisher_t discovery_publisher;
rcl_publisher_t execution_publisher_be;
rcl_publisher_t execution_publisher_rel;
rcl_publisher_t *active_execution_publisher = NULL;
rcl_publisher_t feedback_publisher;

// Subscribers
rcl_subscription_t wake_up_subscriber;
rcl_subscription_t activation_subscriber;
rcl_subscription_t execution_subscriber_be;
rcl_subscription_t execution_subscriber_rel;
rcl_subscription_t test_finished_subscriber;
rcl_subscription_t request_feedback_subscriber;

// Received message structures
static std_msgs__msg__Empty msg_wakeup;
static std_msgs__msg__Empty msg_finished;
static std_msgs__msg__Empty msg_request_feedback;
static std_msgs__msg__String msg_activate;
static uros_custom_msgs__msg__Timestamp received_Timestamp_msg;
static uros_custom_msgs__msg__LatencyReport feedback_msg;

/*
 * Reboots the ESP32 after a MAC-derived delay to avoid
 * concurrent Wi-Fi reconnection bursts across multiple nodes.
 */
void staggered_esp_restart(void)
{
    uint32_t cpu_stagger_ms = (my_mac_tail % 5) * 200;
    vTaskDelay(pdMS_TO_TICKS(cpu_stagger_ms));
    esp_restart();
}

/*
 * Callback for the /wakeup topic. Transitions the node from IDLE
 * to WAITING_ACTIVATION and publishes the device namespace to /discovery.
 */
void wake_up_subscriber_callback(const void *msgin)
{
    if (current_state != STATE_IDLE)
    {
        return;
    }

    ESP_LOGI(device_namespace, "Wake up received.");

    activation_start_time = xTaskGetTickCount();
    current_state = STATE_WAITING_ACTIVATION;

    // Stagger discovery announcements to prevent Wi-Fi channel contention
    vTaskDelay(pdMS_TO_TICKS((my_mac_tail % 5) * 200));

    std_msgs__msg__String device_advertise = {0};
    rosidl_runtime_c__String__assign(&device_advertise.data, device_namespace);

    rcl_ret_t rc = rcl_publish(&discovery_publisher, &device_advertise, NULL);
    if (rc != RCL_RET_OK)
    {
        ESP_LOGE(device_namespace, "Failed to publish discovery (%d)", rc);
        return;
    }
}

/*
 * Parses test parameters from host PC (frequency, sample count, QoS)
 * and configures the active publisher before entering the RUNNING state.
 */
void activation_subscriber_callback(const void *msgin)
{
    if (current_state != STATE_WAITING_ACTIVATION)
    {
        return;
    }

    const std_msgs__msg__String *cfg = (const std_msgs__msg__String *)msgin;

    sscanf(cfg->data.data, "%u,%u,%u", &current_hz, &target_samples, &qos_mode);

    // Set publisher here to avoid 'if' checks inside the transmission loop
    if (qos_mode == 0)
    {
        active_execution_publisher = &execution_publisher_be;
    }
    else
    {
        active_execution_publisher = &execution_publisher_rel;
    }

    // Limit target samples to MAX_SAMPLES to prevent buffer overflow
    if (target_samples > MAX_SAMPLES)
    {
        target_samples = MAX_SAMPLES;
    }

    ESP_LOGI(device_namespace, "Received Config: %u Hz | %u Samples | QoS: %u",
             (unsigned int)current_hz, (unsigned int)target_samples, (unsigned int)qos_mode);

    current_state = STATE_RUNNING;
}

/*
 * Receives echoed timestamp from the host PC, calculates
 * round-trip latency in microseconds, and stores it in the buffer.
 */
void execution_subscriber_callback(const void *msgin)
{
    int64_t arrive_time = esp_timer_get_time();
    int64_t total_latency = arrive_time - ((uros_custom_msgs__msg__Timestamp *)msgin)->timestamp;

    if (current_sample_index < target_samples)
    {
        latency_buffer[current_sample_index++] = (int32_t)total_latency;
    }
}

/*
 * Handles feedback request from host PC. Sets the authorization flag
 * and transitions to STATE_REPORTING if awaiting synchronization.
 */
void request_feedback_subscriber_callback(const void *msgin)
{
    feedback_authorized = true;

    if (current_state != STATE_WAITING_REPORT_SYNC)
        return;

    ESP_LOGI(device_namespace, "Feedback request received from PC. Starting data reporting...");
    current_state = STATE_REPORTING;
}

/*
 * Handles test completion or abort signal from the host PC
 * and triggers a staggered reboot of the ESP32.
 */
void test_finished_subscriber_callback(const void *msgin)
{
    ESP_LOGW(device_namespace, "Test finished/aborted by PC.");
    staggered_esp_restart();
}

/*
 * Queries FreeRTOS task runtime statistics to obtain total system ticks
 * and cumulative idle time for CPU load calculations.
 */
void capture_cpu_snapshot(uint32_t *total_time, uint32_t *idle_time)
{
    static TaskStatus_t task_status_array[MAX_TASKS_SNAPSHOT];
    UBaseType_t array_size = uxTaskGetNumberOfTasks();

    if (array_size > MAX_TASKS_SNAPSHOT)
    {
        array_size = MAX_TASKS_SNAPSHOT;
    }

    uint32_t current_total = 0;
    array_size = uxTaskGetSystemState(task_status_array, array_size, &current_total);

    *total_time = current_total;
    *idle_time = 0;

    // Accumulate runtime from IDLE tasks across all CPU cores
    for (UBaseType_t i = 0; i < array_size; i++)
    {
        if (strncmp(task_status_array[i].pcTaskName, "IDLE", 4) == 0)
        {
            *idle_time += task_status_array[i].ulRunTimeCounter;
        }
    }
}

/*
 * Main FreeRTOS task executing the micro-ROS benchmark state machine.
 */
void ros_task(void *pvParameters)
{
    uros_custom_msgs__msg__Timestamp msg_tms = {0};
    TickType_t state_timeout_start = 0;

    static TickType_t last_ping_time = 0;

    if (!rosidl_runtime_c__String__assign(&msg_tms.device_id, device_namespace))
    {
        ESP_LOGE(device_namespace, "Failed to assign device_id");
        vTaskDelete(NULL);
        return;
    }

    while (1)
    {
        switch (current_state)
        {
            // Awaits /wakeup trigger while maintaining micro-ROS Agent connectivity
            case STATE_IDLE:
            {
                rcl_ret_t spin_rc = rclc_executor_spin_some(&executor, RCL_MS_TO_NS(100));
                if (spin_rc != RCL_RET_TIMEOUT && spin_rc != RCL_RET_OK)
                {
                    ESP_LOGE(device_namespace, "Executor failed with error: %d", spin_rc);
                }

                // Periodically ping Agent (1000 ms timeout, 7 attempts) to verify DDS session
                if ((xTaskGetTickCount() - last_ping_time) > pdMS_TO_TICKS(5000))
                {
                    last_ping_time = xTaskGetTickCount();

                    if (rmw_uros_ping_agent(1000, 7) != RMW_RET_OK)
                    {
                        ESP_LOGE(device_namespace, "Micro-ROS Agent is unreachable. DDS session lost. Restarting device...");
                        staggered_esp_restart();
                    }
                }

                vTaskDelay(pdMS_TO_TICKS(25));
                break;
            }

            // Awaits test parameters on /activate topic after publishing discovery announcement
            case STATE_WAITING_ACTIVATION:
            {
                rclc_executor_spin_some(&executor, RCL_MS_TO_NS(100));

                // Reboot node if configuration is not received within 12 seconds
                if ((xTaskGetTickCount() - activation_start_time) > pdMS_TO_TICKS(12000))
                {
                    staggered_esp_restart();
                }

                vTaskDelay(pdMS_TO_TICKS(25));
                break;
            }

            // Transmits periodic timestamps, processes echoed packets, and publishes CPU metrics
            case STATE_RUNNING:
            {
                feedback_authorized = false;
                current_sample_index = 0;

                TickType_t xLastWakeTime = xTaskGetTickCount();

                // Transmission period in FreeRTOS ticks based on target frequency
                const TickType_t xFrequency = pdMS_TO_TICKS(1000 / current_hz);

                // Record initial CPU and idle ticks before starting the loop
                capture_cpu_snapshot(&start_total_time, &start_idle_time);

                int64_t loop_start_time_us = esp_timer_get_time();

                // Execution loop: transmit timestamps and receive echoes
                for (int i = 0; i < target_samples; i++)
                {
                    if (current_state != STATE_RUNNING)
                        break;

                    msg_tms.timestamp = esp_timer_get_time();
                    rcl_publish(active_execution_publisher, &msg_tms, NULL);

                    TickType_t target_tick = xLastWakeTime + xFrequency;
                    TickType_t current_tick = xTaskGetTickCount();

                    if (target_tick > current_tick)
                    {
                        // Spin executor while waiting for the next message to process incoming echoes
                        while ((current_tick = xTaskGetTickCount()) < target_tick)
                        {
                            TickType_t remaining_ticks = target_tick - current_tick;
                            rclc_executor_spin_some(&executor, RCL_MS_TO_NS(remaining_ticks * portTICK_PERIOD_MS));
                        }

                        xLastWakeTime = target_tick;
                    }
                    else
                    {
                        // If delayed, process incoming messages briefly and reset timer
                        rclc_executor_spin_some(&executor, RCL_MS_TO_NS(1));
                        xLastWakeTime = xTaskGetTickCount();
                    }
                }

                if (current_state == STATE_RUNNING)
                {
                    int64_t loop_end_time_us = esp_timer_get_time();
                    int32_t loop_duration_ms = (int32_t)((loop_end_time_us - loop_start_time_us) / 1000);
                    ESP_LOGI(device_namespace, "Execution duration: %d ms", loop_duration_ms);

                    uint32_t end_total_time = 0;
                    uint32_t end_idle_time = 0;

                    // Record final CPU ticks immediately after the transmission loop finishes
                    capture_cpu_snapshot(&end_total_time, &end_idle_time);

                    uint32_t cpu_stagger_ms = (my_mac_tail % 5) * 200;
                    vTaskDelay(pdMS_TO_TICKS(cpu_stagger_ms));

                    uint32_t delta_total = end_total_time - start_total_time;
                    uint32_t delta_idle = end_idle_time - start_idle_time;

                    if (delta_total > 0)
                    {
                        // Calculate dual-core CPU load percentage
                        float cpu_load = 100.0 - (((float)delta_idle / ((float)delta_total * 2.0)) * 100.0);

                        // Block 998: store CPU load and execution duration
                        feedback_msg.sample_block = 998;
                        feedback_msg.latency_us.size = 2;
                        feedback_msg.latency_us.data[0] = (int32_t)(cpu_load * 100.0);
                        feedback_msg.latency_us.data[1] = loop_duration_ms;

                        TickType_t start_pub_time = xTaskGetTickCount();
                        rcl_ret_t rc = RCL_RET_ERROR;

                        // Retry publishing report until confirmed or 10-second timeout occurs
                        while (rc != RCL_RET_OK && current_state == STATE_RUNNING)
                        {
                            if ((xTaskGetTickCount() - start_pub_time) > pdMS_TO_TICKS(10000))
                            {
                                ESP_LOGE(device_namespace, "Communication timeout. Restarting device...");
                                staggered_esp_restart();
                            }

                            rc = rcl_publish(&feedback_publisher, &feedback_msg, NULL);

                            if (rc != RCL_RET_OK)
                            {
                                rclc_executor_spin_some(&executor, RCL_MS_TO_NS(10));
                                vTaskDelay(pdMS_TO_TICKS(10));
                            }
                            else
                            {
                                rclc_executor_spin_some(&executor, RCL_MS_TO_NS(50));
                                vTaskDelay(pdMS_TO_TICKS(50));
                            }
                        }
                    }

                    current_state = STATE_COOLDOWN;
                }
                break;
            }

            // Waits 2 seconds while spinning executor to receive delayed packets
            case STATE_COOLDOWN:
            {
                TickType_t cooldown_start = xTaskGetTickCount();
                const TickType_t cooldown_duration = pdMS_TO_TICKS(2000);
                TickType_t current_tick;

                while ((current_tick = xTaskGetTickCount()) - cooldown_start < cooldown_duration)
                {
                    TickType_t remaining_ticks = cooldown_duration - (current_tick - cooldown_start);
                    rclc_executor_spin_some(&executor, RCL_MS_TO_NS(remaining_ticks * portTICK_PERIOD_MS));
                }

                current_state = STATE_WAITING_REPORT_SYNC;
                state_timeout_start = xTaskGetTickCount();
                break;
            }

            // Awaits host authorization before uploading collected latency data
            case STATE_WAITING_REPORT_SYNC:
            {
                if (feedback_authorized)
                {
                    // Stagger uploads to prevent router packet loss from simultaneous transmissions
                    uint32_t stagger_delay_ms = (my_mac_tail % 5) * 500;

                    TickType_t delay_ticks = pdMS_TO_TICKS(stagger_delay_ms);
                    TickType_t start_delay = xTaskGetTickCount();

                    while ((xTaskGetTickCount() - start_delay) < delay_ticks)
                    {
                        rclc_executor_spin_some(&executor, RCL_MS_TO_NS(10));
                        vTaskDelay(pdMS_TO_TICKS(10));
                    }

                    ESP_LOGI(device_namespace, "Feedback request received from PC. Starting data reporting...");
                    current_state = STATE_REPORTING;
                }
                else
                {
                    // Reboot if host does not request data within 6 minutes
                    if ((xTaskGetTickCount() - state_timeout_start) > pdMS_TO_TICKS(360000))
                    {
                        ESP_LOGE(device_namespace, "Timeout waiting for feedback request. Restarting device...");
                        staggered_esp_restart();
                    }

                    rclc_executor_spin_some(&executor, RCL_MS_TO_NS(100));
                    vTaskDelay(pdMS_TO_TICKS(25));
                }

                break;
            }

            // Sends stored latency samples in blocks and notifies host when finished
            case STATE_REPORTING:
            {
                // Split samples into SAMPLES_PER_BLOCK to avoid exceeding maximum message size
                uint32_t total_blocks = (current_sample_index + SAMPLES_PER_BLOCK - 1) / SAMPLES_PER_BLOCK;

                for (uint32_t block = 0; block < total_blocks; block++)
                {
                    if (current_state != STATE_REPORTING)
                        break;

                    size_t start_idx = block * SAMPLES_PER_BLOCK;
                    size_t samples_this_block = SAMPLES_PER_BLOCK;

                    if (start_idx + SAMPLES_PER_BLOCK > current_sample_index)
                    {
                        samples_this_block = current_sample_index - start_idx;
                    }

                    feedback_msg.sample_block = block;
                    feedback_msg.latency_us.size = samples_this_block;

                    memcpy(feedback_msg.latency_us.data,
                        &latency_buffer[start_idx],
                        samples_this_block * sizeof(int32_t));

                    TickType_t start_pub_time = xTaskGetTickCount();
                    rcl_ret_t rc = RCL_RET_ERROR;

                    while (rc != RCL_RET_OK && current_state == STATE_REPORTING)
                    {
                        if ((xTaskGetTickCount() - start_pub_time) > pdMS_TO_TICKS(10000))
                        {
                            ESP_LOGE(device_namespace, "Communication timeout. Restarting device...");
                            staggered_esp_restart();
                        }

                        rc = rcl_publish(&feedback_publisher, &feedback_msg, NULL);
                        if (rc != RCL_RET_OK)
                        {
                            rclc_executor_spin_some(&executor, RCL_MS_TO_NS(10));
                            vTaskDelay(pdMS_TO_TICKS(10));
                        }
                        else
                        {
                            rclc_executor_spin_some(&executor, RCL_MS_TO_NS(20));
                            vTaskDelay(pdMS_TO_TICKS(20));
                        }
                    }

                    if (current_state == STATE_REPORTING)
                    {
                        rclc_executor_spin_some(&executor, RCL_MS_TO_NS(20));
                        vTaskDelay(pdMS_TO_TICKS(30));
                    }
                }

                if (current_state == STATE_REPORTING)
                {
                    // Block 9999: empty message to signal that transmission is finished
                    feedback_msg.sample_block = 9999;
                    feedback_msg.latency_us.size = 0;

                    TickType_t start_pub_time = xTaskGetTickCount();
                    rcl_ret_t rc = RCL_RET_ERROR;

                    while (rc != RCL_RET_OK && current_state == STATE_REPORTING)
                    {
                        if ((xTaskGetTickCount() - start_pub_time) > pdMS_TO_TICKS(10000))
                        {
                            ESP_LOGE(device_namespace, "Communication timeout. Restarting device...");
                            staggered_esp_restart();
                        }

                        rc = rcl_publish(&feedback_publisher, &feedback_msg, NULL);
                        if (rc != RCL_RET_OK)
                        {
                            rclc_executor_spin_some(&executor, RCL_MS_TO_NS(10));
                            vTaskDelay(pdMS_TO_TICKS(10));
                        }
                        else
                        {
                            rclc_executor_spin_some(&executor, RCL_MS_TO_NS(50));
                            vTaskDelay(pdMS_TO_TICKS(50));
                        }
                    }

                    ESP_LOGI(device_namespace, "Upload complete. Total samples sent: %d", current_sample_index);

                    if (current_state == STATE_REPORTING)
                    {
                        current_state = STATE_WAITING_FINISH;
                        state_timeout_start = xTaskGetTickCount();
                    }
                }
                break;
            }

            // Awaits /test_finished signal from host PC before restarting
            case STATE_WAITING_FINISH:
            {
                // Reboot if /test_finished is not received within 2 minutes
                if ((xTaskGetTickCount() - state_timeout_start) > pdMS_TO_TICKS(120000))
                {
                    ESP_LOGE(device_namespace, "Timeout waiting for /test_finished. Restarting device...");
                    staggered_esp_restart();
                }

                rclc_executor_spin_some(&executor, RCL_MS_TO_NS(100));
                vTaskDelay(pdMS_TO_TICKS(25));
                break;
            }
        }
    }
}

/*
 * Application entry point. Configures Wi-Fi hardware parameters, establishes
 * the micro-ROS XRCE-DDS session, initializes ROS 2 entities with QoS profiles,
 * and launches the benchmark state machine task.
 */
void app_main(void)
{
    // Initialize the micro-ROS network interface
    ESP_ERROR_CHECK(uros_network_interface_initialize());

    // Disable Wi-Fi power save mode to reduce communication latency
    ESP_ERROR_CHECK(esp_wifi_set_ps(WIFI_PS_NONE));

    // Increase Wi-Fi signal strength by setting the maximum TX power to 19.5 dBm.
    esp_wifi_set_max_tx_power(78);

    // Get the MAC address to generate a unique device ID
    uint8_t mac[6];
    esp_err_t err = esp_read_mac(mac, ESP_MAC_WIFI_STA);

    if (err != ESP_OK)
    {
        ESP_LOGE("BOOT", "Failed to read MAC: %s", esp_err_to_name(err));
        return;
    }

    my_mac_tail = mac[5];

    // Format the device namespace using the last two bytes of the MAC address
    snprintf(device_namespace, sizeof(device_namespace), "ESP32_%02X%02X", mac[4], mac[5]);
    ESP_LOGI("BOOT", "Device namespace: %s", device_namespace);

    // Get the default micro-ROS memory allocator
    allocator = rcl_get_default_allocator();

    // Initialize and configure the initialization options
    rcl_init_options_t init_options = rcl_get_zero_initialized_init_options();
    rcl_ret_t rc = rcl_init_options_init(&init_options, allocator);

    if (rc != RCL_RET_OK)
        return;

    // Apply the configured agent IP and port to the micro-ROS options
    rmw_init_options_t *rmw_options = rcl_init_options_get_rmw_init_options(&init_options);

    // Create a random client key
    uint32_t client_key = esp_random();
    rmw_uros_options_set_client_key(client_key, rmw_options);

    rc = rmw_uros_options_set_udp_address(CONFIG_MICRO_ROS_AGENT_IP, CONFIG_MICRO_ROS_AGENT_PORT, rmw_options);

    if (rc != RMW_RET_OK)
        return;

    // Block until the Micro-ROS Agent is reachable
    while (rmw_uros_ping_agent_options(1000, 1, rmw_options) != RMW_RET_OK)
    {
        ESP_LOGW(device_namespace, "Waiting for Micro-ROS Agent...");
        vTaskDelay(pdMS_TO_TICKS(1000));
    }

    vTaskDelay(pdMS_TO_TICKS((my_mac_tail % 5) * 1000));

    // Initialize the support structure with the configured options
    rc = rclc_support_init_with_options(&support, 0, NULL, &init_options, &allocator);
    if (rc != RCL_RET_OK)
        return;

    // Initialize the node with the unique device namespace
    rc = rclc_node_init_default(&node, device_namespace, "", &support);
    if (rc != RCL_RET_OK)
        return;

    // Format the topic names dynamically based on the device namespace
    static char subscribe_timestamp_topic_be[64];
    snprintf(subscribe_timestamp_topic_be, sizeof(subscribe_timestamp_topic_be), "/%s/timestamp_be_rx", device_namespace);

    static char subscribe_timestamp_topic_rel[64];
    snprintf(subscribe_timestamp_topic_rel, sizeof(subscribe_timestamp_topic_rel), "/%s/timestamp_rel_rx", device_namespace);

    char subscribe_activate_topic[32];
    snprintf(subscribe_activate_topic, sizeof(subscribe_activate_topic), "/%s/activate", device_namespace);

    // Get the type support structures for all used message types
    const rosidl_message_type_support_t *type_support_empty = ROSIDL_GET_MSG_TYPE_SUPPORT(std_msgs, msg, Empty);
    const rosidl_message_type_support_t *type_support_string = ROSIDL_GET_MSG_TYPE_SUPPORT(std_msgs, msg, String);
    const rosidl_message_type_support_t *type_support_timestamp = ROSIDL_GET_MSG_TYPE_SUPPORT(uros_custom_msgs, msg, Timestamp);
    const rosidl_message_type_support_t *type_support_latency = ROSIDL_GET_MSG_TYPE_SUPPORT(uros_custom_msgs, msg, LatencyReport);

    // Initialize publishers for discovery, execution, and feedback
    rc = rclc_publisher_init_default(&discovery_publisher, &node, type_support_string, "/discovery");
    rc = rclc_publisher_init(&execution_publisher_be, &node, type_support_timestamp, "/timestamp_be_tx", &rmw_qos_profile_sensor_data);
    rc = rclc_publisher_init(&execution_publisher_rel, &node, type_support_timestamp, "/timestamp_rel_tx", &rmw_qos_profile_default);
    rc = rclc_publisher_init_default(&feedback_publisher, &node, type_support_latency, "/feedback");

    // Initialize subscribers for wakeup, activation, mirrored execution, and test completion
    rc = rclc_subscription_init_default(&wake_up_subscriber, &node, type_support_empty, "/wakeup");
    rc = rclc_subscription_init_default(&activation_subscriber, &node, type_support_string, subscribe_activate_topic);
    rc = rclc_subscription_init(&execution_subscriber_be, &node, type_support_timestamp, subscribe_timestamp_topic_be, &rmw_qos_profile_sensor_data);
    rc = rclc_subscription_init(&execution_subscriber_rel, &node, type_support_timestamp, subscribe_timestamp_topic_rel, &rmw_qos_profile_default);
    rc = rclc_subscription_init_default(&test_finished_subscriber, &node, type_support_empty, "/test_finished");
    rc = rclc_subscription_init_default(&request_feedback_subscriber, &node, type_support_empty, "/request_feedback");

    if (rc != RCL_RET_OK)
    {
        ESP_LOGE(device_namespace, "Something went wrong: %d", rc);
        return;
    }

    rc = rclc_executor_init(&executor, &support.context, 6, &allocator);
    if (rc != RCL_RET_OK)
    {
        ESP_LOGE(device_namespace, "Failed to initialize executor: %d", rc);
        return;
    }

    // Initialize memory for the incoming timestamp message structure
    uros_custom_msgs__msg__Timestamp__init(&received_Timestamp_msg);
    received_Timestamp_msg.device_id.capacity = 16;
    received_Timestamp_msg.device_id.data = (char *)malloc(16 * sizeof(char));
    received_Timestamp_msg.device_id.size = 0;

    // Initialize memory for the incoming activation message structure
    std_msgs__msg__String__init(&msg_activate);
    msg_activate.data.capacity = 32;
    msg_activate.data.data = (char *)malloc(32);
    msg_activate.data.size = 0;

    // Initialize memory for the incoming empty messages
    std_msgs__msg__Empty__init(&msg_wakeup);
    std_msgs__msg__Empty__init(&msg_finished);
    std_msgs__msg__Empty__init(&msg_request_feedback);

    // Assign the device ID to the feedback message
    rosidl_runtime_c__String__assign(&feedback_msg.device_id, device_namespace);

    // Initialize the latency sequence with the maximum number of entries
    bool latency_sequence_initialized = rosidl_runtime_c__int32__Sequence__init(&feedback_msg.latency_us, 100);
    if (!latency_sequence_initialized)
        return;

    // Add all active subscriptions to the executor
    rc = rclc_executor_add_subscription(&executor, &wake_up_subscriber, &msg_wakeup, wake_up_subscriber_callback, ON_NEW_DATA);
    rc = rclc_executor_add_subscription(&executor, &activation_subscriber, &msg_activate, activation_subscriber_callback, ON_NEW_DATA);
    rc = rclc_executor_add_subscription(&executor, &execution_subscriber_be, &received_Timestamp_msg, execution_subscriber_callback, ON_NEW_DATA);
    rc = rclc_executor_add_subscription(&executor, &execution_subscriber_rel, &received_Timestamp_msg, execution_subscriber_callback, ON_NEW_DATA);
    rc = rclc_executor_add_subscription(&executor, &test_finished_subscriber, &msg_finished, test_finished_subscriber_callback, ON_NEW_DATA);
    rc = rclc_executor_add_subscription(&executor, &request_feedback_subscriber, &msg_request_feedback, request_feedback_subscriber_callback, ON_NEW_DATA);

    if (rc != RCL_RET_OK)
    {
        ESP_LOGE(device_namespace, "Failed to add subscriptions to the executor: %d", rc);
        return;
    }

    // Allocate memory for the latency data buffer
    latency_buffer = (int32_t *)malloc(MAX_SAMPLES * sizeof(int32_t));

    if (latency_buffer == NULL)
    {
        ESP_LOGE(device_namespace, "CRITICAL ERROR: Insufficient RAM for %d samples.", MAX_SAMPLES);
        return;
    }

    // Create the FreeRTOS task to run the state machine and executor spin loop
    BaseType_t xResult = xTaskCreate(ros_task, "ros_task", 8192, NULL, 5, NULL);
    if (xResult != pdPASS)
        return;

    ESP_LOGI(device_namespace, "Micro-ROS node initialized. Entering idle state.");
}